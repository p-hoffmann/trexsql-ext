import type {
  KernelPlugin,
  KernelConfig,
  KernelOutput,
  KernelStatus,
  WebRKernelConfig,
} from '../types'
import { KernelConnectionError } from '../types'
import strategusSpecBuilderSource from './StrategusSpecBuilder.R?raw'
// rD2E source is injected at build time via Vite define (__RD2E_SOURCE__)
// because ?raw uses template literals which corrupt R escape sequences.
declare const __RD2E_SOURCE__: string
const rD2ESource: string = typeof __RD2E_SOURCE__ !== 'undefined' ? __RD2E_SOURCE__ : ''

export class WebRKernel implements KernelPlugin {
  readonly id = 'webr'
  readonly name = 'R (WebR)'
  readonly languages: ReadonlyArray<'python' | 'r'> = ['r']

  private _status: KernelStatus = 'disconnected'
  private statusCallbacks: Set<(status: KernelStatus) => void> = new Set()
  private webR: unknown = null
  private config: WebRKernelConfig | null = null
  private executionCount = 0
  private executionAborted = false

  get status(): KernelStatus {
    return this._status
  }

  private setStatus(status: KernelStatus) {
    this._status = status
    this.statusCallbacks.forEach((cb) => cb(status))
  }

  async connect(config: KernelConfig): Promise<void> {
    if (config.type !== 'webr') {
      throw new KernelConnectionError('Invalid config type for WebRKernel')
    }

    this.config = config
    this.setStatus('connecting')

    try {
      const { WebR } = await import('webr')

      this.webR = new WebR()

      await (this.webR as { init: () => Promise<void> }).init()

      // Set R environment variables (e.g. TREX__ENDPOINT_URL, TREX__AUTHORIZATION_TOKEN)
      if (config.envVars && Object.keys(config.envVars).length > 0) {
        try {
          const envEntries = Object.entries(config.envVars)
            .map(
              ([key, value]) =>
                `Sys.setenv("${key}" = "${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}")`
            )
            .join('\n')
          await (
            this.webR as {
              evalRVoid: (code: string) => Promise<void>
            }
          ).evalRVoid(envEntries)
        } catch (e) {
          console.warn('Failed to set R environment variables:', e)
        }
      }

      if (config.preloadPackages && config.preloadPackages.length > 0) {
        for (const pkg of config.preloadPackages) {
          try {
            await (
              this.webR as {
                evalRVoid: (code: string) => Promise<void>
              }
            ).evalRVoid(`webr::install("${pkg}")`)
          } catch (e) {
            console.warn(`Failed to install ${pkg}:`, e)
          }
        }
      }

      // Autoload Strategus spec builder library
      try {
        // Install dependencies (checkmate for StrategusSpecBuilder.R, jsonlite for rD2E.R)
        await (
          this.webR as {
            evalRVoid: (code: string) => Promise<void>
          }
        ).evalRVoid(`webr::install(c("checkmate", "jsonlite"))`)
        // Source the spec builder functions into the global environment
        await (
          this.webR as {
            evalRVoid: (code: string) => Promise<void>
          }
        ).evalRVoid(strategusSpecBuilderSource)
        // Shim library() so library(Strategus) succeeds without a real installed package.
        // The functions are already in the global env from sourcing above.
        await (
          this.webR as {
            evalRVoid: (code: string) => Promise<void>
          }
        ).evalRVoid(`
local({
  shimmed <- c("Strategus", "rD2E", "CohortMethod", "FeatureExtraction", "Cyclops", "CohortSurvival", "SelfControlledCaseSeries", "PatientLevelPrediction", "EvidenceSynthesis", "CohortIncidence", "Characterization")
  base_env <- as.environment("package:base")

  # Shim library()
  orig_library <- base::library
  library_shim <- function(package, ...) {
    pkg <- tryCatch(as.character(substitute(package)), error = function(e) "")
    if (pkg %in% shimmed) {
      return(invisible(pkg))
    }
    tryCatch(
      orig_library(package = pkg, character.only = TRUE, ...),
      error = function(e) {
        message(paste0("Installing ", pkg, "..."))
        webr::install(pkg)
        orig_library(package = pkg, character.only = TRUE, ...)
      }
    )
  }
  unlockBinding("library", base_env)
  assign("library", library_shim, envir = base_env)
  lockBinding("library", base_env)

  # Shim require()
  orig_require <- base::require
  require_shim <- function(package, ...) {
    pkg <- tryCatch(as.character(substitute(package)), error = function(e) "")
    if (pkg %in% shimmed) {
      return(invisible(TRUE))
    }
    orig_require(package = pkg, character.only = TRUE, ...)
  }
  unlockBinding("require", base_env)
  assign("require", require_shim, envir = base_env)
  lockBinding("require", base_env)

  # Shim :: so rD2E::fn and Strategus::fn resolve from .GlobalEnv
  # (avoids loadNamespace which requires a real installed package)
  dcolon_shim <- function(pkg, name) {
    pkg_str <- as.character(substitute(pkg))
    name_str <- as.character(substitute(name))
    if (pkg_str %in% shimmed) {
      # Check for package-specific override first (e.g. .sccs_createCreateStudyPopulationArgs)
      pkg_specific <- paste0(".", pkg_str, "_", name_str)
      if (exists(pkg_specific, envir = .GlobalEnv)) {
        return(get(pkg_specific, envir = .GlobalEnv))
      }
      if (exists(name_str, envir = .GlobalEnv)) {
        return(get(name_str, envir = .GlobalEnv))
      }
    }
    getExportedValue(asNamespace(pkg_str), name_str)
  }
  unlockBinding("::", base_env)
  assign("::", dcolon_shim, envir = base_env)
  lockBinding("::", base_env)

  # Shim ::: similarly
  tcolon_shim <- function(pkg, name) {
    pkg_str <- as.character(substitute(pkg))
    name_str <- as.character(substitute(name))
    if (pkg_str %in% shimmed) {
      pkg_specific <- paste0(".", pkg_str, "_", name_str)
      if (exists(pkg_specific, envir = .GlobalEnv)) {
        return(get(pkg_specific, envir = .GlobalEnv))
      }
      if (exists(name_str, envir = .GlobalEnv)) {
        return(get(name_str, envir = .GlobalEnv))
      }
    }
    get(name_str, envir = asNamespace(pkg_str))
  }
  unlockBinding(":::", base_env)
  assign(":::", tcolon_shim, envir = base_env)
  lockBinding(":::", base_env)
})
`)
      } catch (e) {
        console.warn('Failed to load Strategus spec builder:', e)
      }

      // Autoload rD2E library (WebR-compatible port — no external deps needed)
      // Source the rD2E functions, then attach to the search path so they're
      // accessible from Shelter.captureR() which may use a different env.
      try {
        await (
          this.webR as {
            evalRVoid: (code: string) => Promise<void>
          }
        ).evalRVoid(rD2ESource)
        // Attach rD2E functions to the search path
        await (
          this.webR as {
            evalRVoid: (code: string) => Promise<void>
          }
        ).evalRVoid(`local({
  rD2E_fns <- c("get_cohort_definition_set", "create_cohort_definition",
                "run_strategus_flow", "create_options",
                ".rD2E_to_json", ".rD2E_from_json",
                ".rD2E_GET", ".rD2E_POST",
                ".rD2E_js_escape",
                ".rD2E_getCohortDefinition", ".rD2E_getDeployment")
  env <- new.env(parent = emptyenv())
  for (fn in rD2E_fns) {
    if (exists(fn, envir = .GlobalEnv)) {
      assign(fn, get(fn, envir = .GlobalEnv), envir = env)
    }
  }
  attach(env, name = "rD2E")
})`)
      } catch (e) {
        console.error('Failed to load rD2E library:', e)
      }

      this.setStatus('idle')
    } catch (error) {
      this.setStatus('error')
      throw new KernelConnectionError(
        error instanceof Error ? error.message : 'Failed to initialize WebR',
        error instanceof Error ? error : undefined
      )
    }
  }

  async disconnect(): Promise<void> {
    if (this.webR) {
      try {
        await (this.webR as { close: () => Promise<void> }).close()
      } catch {
        // ignore
      }
      this.webR = null
    }
    this.setStatus('disconnected')
  }

  async *execute(code: string, language: 'python' | 'r'): AsyncIterable<KernelOutput> {
    if (language !== 'r') {
      throw new Error('WebRKernel only supports R')
    }

    if (!this.webR || this._status === 'disconnected') {
      throw new Error('Kernel is not connected')
    }

    this.executionCount++
    const execCount = this.executionCount
    this.executionAborted = false
    this.setStatus('busy')

    try {
      const webR = this.webR as {
        evalR: (code: string) => Promise<unknown>
        evalRVoid: (code: string) => Promise<void>
        Shelter: new () => Promise<{
          captureR: (
            code: string,
            options?: { env?: unknown }
          ) => Promise<{
            output: Array<{ type: string; data: string }>
            images: string[]
            result: unknown
          }>
          purge: () => void
        }>
      }

      // Pre-install packages referenced by library()/require() calls so that
      // installation happens outside captureR(). This prevents long package
      // downloads (e.g. dplyr with ~15 deps) from running inside captureR()
      // where a timeout would corrupt the Shelter state.
      const libraryPattern = /(?:library|require)\s*\(\s*(?:["']([^"']+)["']|(\w+))/g
      let match
      while ((match = libraryPattern.exec(code)) !== null) {
        const pkg = match[1] || match[2]
        if (pkg) {
          try {
            await webR.evalRVoid(
              `if (!requireNamespace("${pkg}", quietly = TRUE)) webr::install("${pkg}")`
            )
          } catch {
            // Let captureR handle the error naturally
          }
          if (this.executionAborted) return
        }
      }

      if (this.executionAborted) return

      // Shelter provides safe R evaluation with automatic cleanup
      const Shelter = await new webR.Shelter()

      try {
        const result = await Shelter.captureR(code)

        if (this.executionAborted) return

        for (const output of result.output) {
          yield {
            type: 'stream',
            name: output.type === 'stderr' ? 'stderr' : 'stdout',
            text: output.data,
          } as KernelOutput
        }

        for (const imageData of result.images) {
          const img = imageData
          yield {
            type: 'display_data',
            data: {
              'image/png': img,
            },
          } as KernelOutput
        }

        if (result.result !== null && result.result !== undefined) {
          try {
            const proxy = result.result as Record<string, unknown>
            let resultStr: string
            // WebR proxy objects may have toJs/toArray; plain objects may have {values}
            if (typeof proxy.toArray === 'function') {
              const arr = await (proxy.toArray as () => Promise<unknown[]>)()
              resultStr = arr.map(String).join('\n')
            } else if (typeof proxy.toJs === 'function') {
              const jsVal = await (proxy.toJs as () => Promise<unknown>)()
              if (jsVal && typeof jsVal === 'object' && 'values' in jsVal) {
                const vals = (jsVal as { values: unknown }).values
                resultStr = Array.isArray(vals) ? vals.map(String).join('\n') : String(vals)
              } else if (Array.isArray(jsVal)) {
                resultStr = jsVal.map(String).join('\n')
              } else {
                resultStr = String(jsVal)
              }
            } else {
              resultStr = String(proxy)
            }
            if (resultStr && resultStr !== '[object Object]' && resultStr !== 'undefined') {
              yield {
                type: 'execute_result',
                executionCount: execCount,
                data: {
                  'text/plain': resultStr,
                },
              } as KernelOutput
            }
          } catch {
            // proxy can't be converted
          }
        }
      } finally {
        try {
          Shelter.purge()
        } catch {
          // Shelter may be invalid if WebR was destroyed by interrupt
        }
      }

      if (!this.executionAborted) {
        this.setStatus('idle')
      }
    } catch (error) {
      if (this.executionAborted) return

      const errorMessage = error instanceof Error ? error.message : String(error)
      const traceback = error instanceof Error && error.stack ? error.stack.split('\n') : []

      yield {
        type: 'error',
        ename: error instanceof Error ? error.constructor.name : 'Error',
        evalue: errorMessage,
        traceback,
      } as KernelOutput

      this.setStatus('idle')
    }
  }

  async interrupt(): Promise<void> {
    this.executionAborted = true

    if (this.webR) {
      try {
        await (this.webR as { interrupt: () => void }).interrupt()
        this.setStatus('idle')
      } catch {
        // Interrupt failed, recreate the kernel
        // connect() will set status to 'idle' on success
        await this.disconnect()
        if (this.config) {
          await this.connect(this.config)
        }
      }
    }
  }

  onStatusChange(callback: (status: KernelStatus) => void): () => void {
    this.statusCallbacks.add(callback)
    return () => {
      this.statusCallbacks.delete(callback)
    }
  }
}

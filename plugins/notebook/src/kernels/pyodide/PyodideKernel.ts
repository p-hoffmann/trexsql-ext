import type {
  KernelPlugin,
  KernelConfig,
  KernelOutput,
  KernelStatus,
  PyodideKernelConfig,
} from '../types'
import { KernelConnectionError, KernelInterruptError } from '../types'
import type { WorkerRequest, WorkerResponse } from './pyodide-worker'

export class PyodideKernel implements KernelPlugin {
  readonly id = 'pyodide'
  readonly name = 'Python (Pyodide)'
  readonly languages: ReadonlyArray<'python' | 'r'> = ['python']

  private _status: KernelStatus = 'disconnected'
  private statusCallbacks: Set<(status: KernelStatus) => void> = new Set()
  private worker: Worker | null = null
  private messageId = 0
  private pendingExecutions: Map<
    string,
    {
      resolve: () => void
      reject: (error: Error) => void
      outputs: KernelOutput[]
      outputCallback?: (output: KernelOutput) => void
    }
  > = new Map()
  private config: PyodideKernelConfig | null = null

  get status(): KernelStatus {
    return this._status
  }

  private setStatus(status: KernelStatus) {
    this._status = status
    this.statusCallbacks.forEach((cb) => cb(status))
  }

  async connect(config: KernelConfig): Promise<void> {
    if (config.type !== 'pyodide') {
      throw new KernelConnectionError('Invalid config type for PyodideKernel')
    }

    // Guard against re-entrant connect calls
    if (this._status === 'connecting') {
      return
    }

    // Clean up existing worker before creating a new one
    if (this.worker) {
      this.worker.terminate()
      this.worker = null
    }

    this.config = config
    this.setStatus('connecting')

    return new Promise((resolve, reject) => {
      try {
        this.worker = new Worker(
          new URL('./pyodide-worker.ts', import.meta.url),
          { type: 'module' }
        )

        this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
          this.handleWorkerMessage(event.data)
        }

        this.worker.onerror = (error) => {
          this.setStatus('error')
          reject(new KernelConnectionError(`Worker error: ${error.message}`))
        }

        const readyHandler = (event: MessageEvent<WorkerResponse>) => {
          if (event.data.type === 'ready') {
            this.worker?.removeEventListener('message', readyHandler as EventListener)
            resolve()
          } else if (event.data.type === 'error' && !event.data.id) {
            this.worker?.removeEventListener('message', readyHandler as EventListener)
            const errorData = event.data.data as { evalue?: string }
            reject(new KernelConnectionError(errorData?.evalue || 'Failed to initialize Pyodide'))
          }
        }

        this.worker.addEventListener('message', readyHandler as EventListener)

        const initMessage: WorkerRequest = {
          type: 'init',
          id: 'init',
          indexUrl: config.indexUrl,
          preloadPackages: config.preloadPackages,
          envVars: config.envVars,
        }
        this.worker.postMessage(initMessage)
      } catch (error) {
        this.setStatus('error')
        reject(new KernelConnectionError(
          error instanceof Error ? error.message : 'Failed to create worker'
        ))
      }
    })
  }

  async disconnect(): Promise<void> {
    if (this.worker) {
      this.worker.terminate()
      this.worker = null
    }
    this.pendingExecutions.clear()
    this.setStatus('disconnected')
  }

  async *execute(code: string, language: 'python' | 'r'): AsyncIterable<KernelOutput> {
    if (language !== 'python') {
      throw new Error('PyodideKernel only supports Python')
    }

    if (!this.worker || this._status === 'disconnected') {
      throw new Error('Kernel is not connected')
    }

    const id = `exec-${++this.messageId}`
    const outputs: KernelOutput[] = []
    let resolveExecution: () => void
    let rejectExecution: (error: Error) => void

    const executionPromise = new Promise<void>((resolve, reject) => {
      resolveExecution = resolve
      rejectExecution = reject
    })

    this.pendingExecutions.set(id, {
      resolve: resolveExecution!,
      reject: rejectExecution!,
      outputs,
      outputCallback: (output) => {
        outputs.push(output)
      },
    })

    const request: WorkerRequest = {
      type: 'execute',
      id,
      code,
    }
    this.worker.postMessage(request)

    let lastOutputIndex = 0
    const checkInterval = 50

    try {
      while (true) {
        while (lastOutputIndex < outputs.length) {
          yield outputs[lastOutputIndex++]
        }

        if (!this.pendingExecutions.has(id)) {
          while (lastOutputIndex < outputs.length) {
            yield outputs[lastOutputIndex++]
          }
          break
        }

        await new Promise((resolve) => setTimeout(resolve, checkInterval))
      }

      await executionPromise
    } catch (error) {
      if (error instanceof KernelInterruptError) {
        throw error
      }
      throw error
    }
  }

  async interrupt(): Promise<void> {
    if (!this.worker) return

    // Terminate and recreate worker since there's no graceful interrupt
    this.worker.terminate()

    for (const [, execution] of this.pendingExecutions) {
      execution.reject(new KernelInterruptError())
    }
    this.pendingExecutions.clear()

    if (this.config) {
      await this.connect(this.config)
    }
  }

  onStatusChange(callback: (status: KernelStatus) => void): () => void {
    this.statusCallbacks.add(callback)
    return () => {
      this.statusCallbacks.delete(callback)
    }
  }

  private handleWorkerMessage(msg: WorkerResponse) {
    const execution = msg.id ? this.pendingExecutions.get(msg.id) : null

    switch (msg.type) {
      case 'status': {
        const statusData = msg.data as { state: KernelStatus }
        this.setStatus(statusData.state)
        break
      }

      case 'stdout': {
        if (execution) {
          const output: KernelOutput = {
            type: 'stream',
            name: 'stdout',
            text: String(msg.data),
          }
          execution.outputCallback?.(output)
        }
        break
      }

      case 'stderr': {
        if (execution) {
          const output: KernelOutput = {
            type: 'stream',
            name: 'stderr',
            text: String(msg.data),
          }
          execution.outputCallback?.(output)
        }
        break
      }

      case 'result': {
        if (execution) {
          const output: KernelOutput = {
            type: 'execute_result',
            executionCount: 0, // Will be set by caller
            data: msg.data as Record<string, unknown>,
          }
          execution.outputCallback?.(output)
        }
        break
      }

      case 'display_data': {
        if (execution) {
          const output: KernelOutput = {
            type: 'display_data',
            data: msg.data as Record<string, unknown>,
          }
          execution.outputCallback?.(output)
        }
        break
      }

      case 'error': {
        if (execution) {
          const errorData = msg.data as { ename: string; evalue: string; traceback: string[] }
          const output: KernelOutput = {
            type: 'error',
            ename: errorData.ename,
            evalue: errorData.evalue,
            traceback: errorData.traceback,
          }
          execution.outputCallback?.(output)

          this.pendingExecutions.delete(msg.id)
          execution.resolve()
        }
        break
      }

      case 'ready':
        break
    }

    // Execution completes when status returns to idle
    if (msg.type === 'status' && execution) {
      const statusData = msg.data as { state: KernelStatus }
      if (statusData.state === 'idle') {
        this.pendingExecutions.delete(msg.id)
        execution.resolve()
      }
    }
  }
}

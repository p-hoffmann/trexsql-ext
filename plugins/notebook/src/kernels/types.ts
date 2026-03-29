import type { MimeBundle } from '@/types/notebook'

export type KernelStatus = 'disconnected' | 'connecting' | 'idle' | 'busy' | 'error'

export interface PyodideKernelConfig {
  type: 'pyodide'
  preloadPackages?: string[]
  indexUrl?: string
  /** Environment variables to set in the Python os.environ (e.g. PYQE_URL, TOKEN) */
  envVars?: Record<string, string>
}

export interface WebRKernelConfig {
  type: 'webr'
  preloadPackages?: string[]
  /** Environment variables to set in the R environment (e.g. TREX__ENDPOINT_URL, TREX__AUTHORIZATION_TOKEN) */
  envVars?: Record<string, string>
}

export interface JupyterKernelConfig {
  type: 'jupyter'
  serverUrl: string
  token: string
  kernelId: string
  kernelName?: string
  sessionName?: string
}

export type KernelConfig = PyodideKernelConfig | WebRKernelConfig | JupyterKernelConfig

export interface StreamOutput {
  type: 'stream'
  name: 'stdout' | 'stderr'
  text: string
}

export interface DisplayDataOutput {
  type: 'display_data'
  data: MimeBundle
  metadata?: Record<string, unknown>
}

export interface ExecuteResultOutput {
  type: 'execute_result'
  executionCount: number
  data: MimeBundle
  metadata?: Record<string, unknown>
}

export interface ErrorOutput {
  type: 'error'
  ename: string
  evalue: string
  traceback: string[]
}

export interface StatusOutput {
  type: 'status'
  state: KernelStatus
}

export type KernelOutput =
  | StreamOutput
  | DisplayDataOutput
  | ExecuteResultOutput
  | ErrorOutput
  | StatusOutput

export class KernelInterruptError extends Error {
  constructor(message = 'Execution interrupted') {
    super(message)
    this.name = 'KernelInterruptError'
  }
}

export class KernelConnectionError extends Error {
  readonly cause?: Error

  constructor(message: string, cause?: Error) {
    super(message)
    this.name = 'KernelConnectionError'
    this.cause = cause
  }
}

export interface KernelPlugin {
  readonly id: string
  readonly name: string
  readonly languages: ReadonlyArray<'python' | 'r'>
  readonly status: KernelStatus

  connect(config: KernelConfig): Promise<void>
  disconnect(): Promise<void>
  execute(code: string, language: 'python' | 'r'): AsyncIterable<KernelOutput>
  interrupt(): Promise<void>
  onStatusChange(callback: (status: KernelStatus) => void): () => void
}

export type KernelFactory = () => KernelPlugin

export interface KernelRegistry {
  register(id: string, factory: KernelFactory): void
  get(id: string): KernelPlugin | undefined
  list(): string[]
  getByLanguage(language: 'python' | 'r'): string[]
}

import type {
  KernelPlugin,
  KernelConfig,
  KernelOutput,
  KernelStatus,
  JupyterKernelConfig,
} from '../types'
import { KernelConnectionError } from '../types'

interface JupyterMessageHeader {
  msg_id: string
  session: string
  msg_type: string
  version: string
  username?: string
  date?: string
}

interface JupyterMessage {
  header: JupyterMessageHeader
  parent_header: Partial<JupyterMessageHeader>
  metadata: Record<string, unknown>
  content: Record<string, unknown>
  channel?: 'shell' | 'iopub' | 'stdin' | 'control'
  buffers?: ArrayBuffer[]
}

function generateUUID(): string {
  return crypto.randomUUID()
}

export class JupyterKernel implements KernelPlugin {
  readonly id = 'jupyter'
  readonly name = 'Jupyter Kernel'
  readonly languages: ReadonlyArray<'python' | 'r'> = ['python', 'r']

  private _status: KernelStatus = 'disconnected'
  private statusCallbacks: Set<(status: KernelStatus) => void> = new Set()
  private ws: WebSocket | null = null
  private config: JupyterKernelConfig | null = null
  private sessionId: string = generateUUID()
  private executionCount = 0

  private pendingExecutions: Map<
    string,
    {
      resolve: () => void
      reject: (error: Error) => void
      outputCallback: (output: KernelOutput) => void
    }
  > = new Map()

  get status(): KernelStatus {
    return this._status
  }

  private setStatus(status: KernelStatus) {
    this._status = status
    this.statusCallbacks.forEach((cb) => cb(status))
  }

  async connect(config: KernelConfig): Promise<void> {
    if (config.type !== 'jupyter') {
      throw new KernelConnectionError('Invalid config type for JupyterKernel')
    }

    this.config = config
    this.setStatus('connecting')

    try {
      const wsProtocol = config.serverUrl.startsWith('https') ? 'wss' : 'ws'
      const serverHost = config.serverUrl.replace(/^https?:\/\//, '')
      const wsUrl = `${wsProtocol}://${serverHost}/api/kernels/${config.kernelId}/channels?token=${config.token}`

      this.ws = new WebSocket(wsUrl)

      await new Promise<void>((resolve, reject) => {
        if (!this.ws) {
          reject(new Error('WebSocket not created'))
          return
        }

        const timeout = setTimeout(() => {
          reject(new Error('WebSocket connection timeout'))
        }, 10000)

        this.ws.onopen = () => {
          clearTimeout(timeout)
          resolve()
        }

        this.ws.onerror = (event) => {
          clearTimeout(timeout)
          reject(new Error(`WebSocket error: ${event}`))
        }
      })

      this.ws.onmessage = (event) => {
        this.handleMessage(event)
      }

      this.ws.onclose = () => {
        this.setStatus('disconnected')
        this.ws = null
      }

      this.ws.onerror = () => {
        this.setStatus('error')
      }

      this.setStatus('idle')
    } catch (error) {
      this.setStatus('error')
      throw new KernelConnectionError(
        error instanceof Error ? error.message : 'Failed to connect to Jupyter kernel',
        error instanceof Error ? error : undefined
      )
    }
  }

  async disconnect(): Promise<void> {
    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
    this.pendingExecutions.clear()
    this.setStatus('disconnected')
  }

  async *execute(code: string, /* language: 'python' | 'r' */): AsyncIterable<KernelOutput> {
    if (!this.ws || this._status === 'disconnected') {
      throw new Error('Kernel is not connected')
    }

    this.executionCount++
    const execCount = this.executionCount
    const msgId = generateUUID()

    const executeRequest: JupyterMessage = {
      header: {
        msg_id: msgId,
        session: this.sessionId,
        msg_type: 'execute_request',
        version: '5.4',
        username: 'trex-notebook',
        date: new Date().toISOString(),
      },
      parent_header: {},
      metadata: {},
      content: {
        code,
        silent: false,
        store_history: true,
        user_expressions: {},
        allow_stdin: false,
        stop_on_error: true,
      },
    }

    this.setStatus('busy')

    const outputQueue: KernelOutput[] = []
    let isComplete = false
    let executeError: Error | null = null
    let resolveWait: (() => void) | null = null

    this.pendingExecutions.set(msgId, {
      resolve: () => {
        isComplete = true
        if (resolveWait) resolveWait()
      },
      reject: (error: Error) => {
        executeError = error
        isComplete = true
        if (resolveWait) resolveWait()
      },
      outputCallback: (output: KernelOutput) => {
        outputQueue.push(output)
        if (resolveWait) resolveWait()
      },
    })

    try {
      this.ws.send(JSON.stringify(executeRequest))

      while (!isComplete || outputQueue.length > 0) {
        if (outputQueue.length > 0) {
          const output = outputQueue.shift()!
          if (output.type === 'execute_result') {
            yield { ...output, executionCount: execCount } as KernelOutput
          } else {
            yield output
          }
        } else if (!isComplete) {
          await new Promise<void>((resolve) => {
            resolveWait = resolve
          })
          resolveWait = null
        }
      }

      if (executeError) {
        throw executeError
      }
    } finally {
      this.pendingExecutions.delete(msgId)
      this.setStatus('idle')
    }
  }

  async interrupt(): Promise<void> {
    if (!this.config) return

    try {
      // Interrupt via REST API (not WebSocket)
      const response = await fetch(
        `${this.config.serverUrl}/api/kernels/${this.config.kernelId}/interrupt?token=${this.config.token}`,
        { method: 'POST' }
      )

      if (!response.ok) {
        throw new Error(`Failed to interrupt kernel: ${response.statusText}`)
      }
    } catch (error) {
      console.error('Failed to interrupt kernel:', error)
    }

    this.setStatus('idle')
  }

  onStatusChange(callback: (status: KernelStatus) => void): () => void {
    this.statusCallbacks.add(callback)
    return () => {
      this.statusCallbacks.delete(callback)
    }
  }

  private handleMessage(event: MessageEvent) {
    let message: JupyterMessage

    try {
      message = JSON.parse(event.data)
    } catch {
      console.warn('Failed to parse Jupyter message:', event.data)
      return
    }

    const parentMsgId = message.parent_header?.msg_id
    if (!parentMsgId) return

    const pending = this.pendingExecutions.get(parentMsgId)
    if (!pending) return

    const { resolve, reject, outputCallback } = pending
    const msgType = message.header.msg_type
    const content = message.content

    switch (msgType) {
      case 'stream':
        outputCallback({
          type: 'stream',
          name: content.name as 'stdout' | 'stderr',
          text: content.text as string,
        })
        break

      case 'display_data':
        outputCallback({
          type: 'display_data',
          data: content.data as Record<string, string>,
          metadata: content.metadata as Record<string, unknown>,
        })
        break

      case 'execute_result':
        outputCallback({
          type: 'execute_result',
          executionCount: content.execution_count as number,
          data: content.data as Record<string, string>,
          metadata: content.metadata as Record<string, unknown>,
        })
        break

      case 'error':
        outputCallback({
          type: 'error',
          ename: content.ename as string,
          evalue: content.evalue as string,
          traceback: content.traceback as string[],
        })
        break

      case 'status':
        if (content.execution_state === 'idle') {
          // execution complete, handled by execute_reply
        } else if (content.execution_state === 'busy') {
          this.setStatus('busy')
        }
        break

      case 'execute_reply':
        if (content.status === 'ok') {
          resolve()
        } else if (content.status === 'error') {
          reject(new Error(`${content.ename}: ${content.evalue}`))
        } else if (content.status === 'aborted') {
          reject(new Error('Execution aborted'))
        }
        break
    }
  }
}

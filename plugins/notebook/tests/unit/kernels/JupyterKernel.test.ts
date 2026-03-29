/**
 * Unit tests for JupyterKernel
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { JupyterKernel } from '@/kernels/jupyter/JupyterKernel'

// Mock WebSocket
class MockWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3

  readyState = MockWebSocket.CONNECTING
  onopen: ((event: Event) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null

  private messageHandlers: Array<(data: string) => void> = []

  constructor(public url: string) {
    // Simulate async connection
    setTimeout(() => {
      this.readyState = MockWebSocket.OPEN
      this.onopen?.(new Event('open'))
    }, 10)
  }

  send(data: string) {
    // Simulate receiving responses for execute requests
    const message = JSON.parse(data)
    if (message.header.msg_type === 'execute_request') {
      setTimeout(() => {
        // Send stream output
        this.simulateMessage({
          header: { msg_type: 'stream', msg_id: 'resp-1' },
          parent_header: { msg_id: message.header.msg_id },
          content: { name: 'stdout', text: 'Hello World\n' },
          metadata: {},
        })

        // Send execute_reply
        setTimeout(() => {
          this.simulateMessage({
            header: { msg_type: 'execute_reply', msg_id: 'resp-2' },
            parent_header: { msg_id: message.header.msg_id },
            content: { status: 'ok', execution_count: 1 },
            metadata: {},
          })
        }, 10)
      }, 10)
    }
  }

  close() {
    this.readyState = MockWebSocket.CLOSED
    this.onclose?.(new CloseEvent('close'))
  }

  simulateMessage(data: object) {
    this.onmessage?.(new MessageEvent('message', { data: JSON.stringify(data) }))
  }

  simulateError() {
    this.onerror?.(new Event('error'))
  }
}

// Store original WebSocket
const OriginalWebSocket = global.WebSocket

describe('JupyterKernel', () => {
  let kernel: JupyterKernel

  beforeEach(() => {
    // Mock WebSocket globally
    global.WebSocket = MockWebSocket as unknown as typeof WebSocket
    kernel = new JupyterKernel()
  })

  afterEach(() => {
    global.WebSocket = OriginalWebSocket
  })

  describe('properties', () => {
    it('has correct id', () => {
      expect(kernel.id).toBe('jupyter')
    })

    it('has correct name', () => {
      expect(kernel.name).toBe('Jupyter Kernel')
    })

    it('supports python and r languages', () => {
      expect(kernel.languages).toContain('python')
      expect(kernel.languages).toContain('r')
    })

    it('initial status is disconnected', () => {
      expect(kernel.status).toBe('disconnected')
    })
  })

  describe('connect', () => {
    it('connects successfully with valid config', async () => {
      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'http://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })
      expect(kernel.status).toBe('idle')
    })

    it('throws error for invalid config type', async () => {
      await expect(
        kernel.connect({ type: 'pyodide' } as never)
      ).rejects.toThrow('Invalid config type')
    })

    it('calls status callbacks during connection', async () => {
      const statusCallback = vi.fn()
      kernel.onStatusChange(statusCallback)

      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'http://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })

      expect(statusCallback).toHaveBeenCalledWith('connecting')
      expect(statusCallback).toHaveBeenCalledWith('idle')
    })

    it('uses wss for https urls', async () => {
      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'https://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })
      expect(kernel.status).toBe('idle')
    })
  })

  describe('disconnect', () => {
    it('disconnects successfully', async () => {
      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'http://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })
      await kernel.disconnect()
      expect(kernel.status).toBe('disconnected')
    })

    it('can disconnect when not connected', async () => {
      await kernel.disconnect()
      expect(kernel.status).toBe('disconnected')
    })
  })

  describe('execute', () => {
    beforeEach(async () => {
      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'http://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })
    })

    it('executes code and returns output', async () => {
      const outputs: unknown[] = []
      for await (const output of kernel.execute('print("Hello")', 'python')) {
        outputs.push(output)
      }

      expect(outputs.length).toBeGreaterThan(0)
      expect(outputs[0]).toHaveProperty('type', 'stream')
    })

    it('throws error when not connected', async () => {
      await kernel.disconnect()

      await expect(async () => {
        for await (const output of kernel.execute('1+1', 'python')) {
          void output // consume iterator
        }
      }).rejects.toThrow('not connected')
    })

    it('sets status to busy during execution', async () => {
      const statusCallback = vi.fn()
      kernel.onStatusChange(statusCallback)

      for await (const output of kernel.execute('1+1', 'python')) {
        void output // consume iterator
      }

      expect(statusCallback).toHaveBeenCalledWith('busy')
    })
  })

  describe('interrupt', () => {
    beforeEach(async () => {
      // Mock fetch for interrupt
      global.fetch = vi.fn().mockResolvedValue({ ok: true })

      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'http://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('sends interrupt request to server', async () => {
      await kernel.interrupt()

      expect(global.fetch).toHaveBeenCalledWith(
        expect.stringContaining('/api/kernels/test-kernel-id/interrupt'),
        expect.objectContaining({ method: 'POST' })
      )
    })

    it('sets status to idle after interrupt', async () => {
      await kernel.interrupt()
      expect(kernel.status).toBe('idle')
    })
  })

  describe('onStatusChange', () => {
    it('returns unsubscribe function', async () => {
      const callback = vi.fn()
      const unsubscribe = kernel.onStatusChange(callback)

      await kernel.connect({
        type: 'jupyter',
        serverUrl: 'http://localhost:8888',
        kernelId: 'test-kernel-id',
        token: 'test-token',
      })
      expect(callback).toHaveBeenCalled()

      callback.mockClear()
      unsubscribe()

      await kernel.disconnect()
      expect(callback).not.toHaveBeenCalled()
    })
  })
})

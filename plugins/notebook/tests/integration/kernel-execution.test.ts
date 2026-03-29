/**
 * Integration tests for kernel execution
 *
 * Note: These tests mock the actual kernel implementations since
 * real Pyodide/WebR require browser environment with workers.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import type { KernelPlugin, KernelOutput, KernelStatus, KernelConfig } from '@/kernels/types'

// Create a test kernel that simulates real behavior
function createTestKernel(): KernelPlugin {
  let status: KernelStatus = 'disconnected'
  const statusCallbacks = new Set<(status: KernelStatus) => void>()

  return {
    id: 'test',
    name: 'Test Kernel',
    languages: ['python', 'r'] as const,
    get status() {
      return status
    },
    async connect() {
      status = 'connecting'
      statusCallbacks.forEach((cb) => cb(status))
      await new Promise((resolve) => setTimeout(resolve, 50))
      status = 'idle'
      statusCallbacks.forEach((cb) => cb(status))
    },
    async disconnect() {
      status = 'disconnected'
      statusCallbacks.forEach((cb) => cb(status))
    },
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    async *execute(code: string, language: 'python' | 'r'): AsyncIterable<KernelOutput> {
      if (status === 'disconnected') {
        throw new Error('Kernel is not connected')
      }
      status = 'busy'
      statusCallbacks.forEach((cb) => cb(status))

      // Simulate processing time
      await new Promise((resolve) => setTimeout(resolve, 10))

      // Parse and execute simple expressions
      if (code.includes('print')) {
        const match = code.match(/print\(["'](.*)["']\)/)
        if (match) {
          yield { type: 'stream', name: 'stdout', text: match[1] + '\n' }
        }
      } else if (code.includes('error')) {
        yield {
          type: 'error',
          ename: 'TestError',
          evalue: 'Simulated error',
          traceback: ['Traceback...', 'TestError: Simulated error'],
        }
      } else if (code.match(/^\d+\s*\+\s*\d+$/)) {
        const result = eval(code)
        yield {
          type: 'execute_result',
          executionCount: 1,
          data: { 'text/plain': String(result) },
        }
      } else if (code.includes('plot')) {
        yield {
          type: 'display_data',
          data: { 'image/png': 'base64encodedimage' },
          metadata: {},
        }
      }

      status = 'idle'
      statusCallbacks.forEach((cb) => cb(status))
    },
    async interrupt() {
      status = 'idle'
      statusCallbacks.forEach((cb) => cb(status))
    },
    onStatusChange(callback: (status: KernelStatus) => void) {
      statusCallbacks.add(callback)
      return () => statusCallbacks.delete(callback)
    },
  }
}

describe('Kernel Execution Integration', () => {
  let kernel: KernelPlugin

  beforeEach(async () => {
    kernel = createTestKernel()
    await kernel.connect({ type: 'test' } as KernelConfig)
  })

  describe('basic execution', () => {
    it('executes print statement and returns stdout', async () => {
      const outputs: KernelOutput[] = []

      for await (const output of kernel.execute('print("Hello World")', 'python')) {
        outputs.push(output)
      }

      expect(outputs).toHaveLength(1)
      expect(outputs[0]).toEqual({
        type: 'stream',
        name: 'stdout',
        text: 'Hello World\n',
      })
    })

    it('executes arithmetic and returns result', async () => {
      const outputs: KernelOutput[] = []

      for await (const output of kernel.execute('2 + 3', 'python')) {
        outputs.push(output)
      }

      expect(outputs).toHaveLength(1)
      expect(outputs[0]).toMatchObject({
        type: 'execute_result',
        data: { 'text/plain': '5' },
      })
    })

    it('handles errors gracefully', async () => {
      const outputs: KernelOutput[] = []

      for await (const output of kernel.execute('raise error', 'python')) {
        outputs.push(output)
      }

      expect(outputs).toHaveLength(1)
      expect(outputs[0]).toMatchObject({
        type: 'error',
        ename: 'TestError',
      })
    })

    it('returns display data for plots', async () => {
      const outputs: KernelOutput[] = []

      for await (const output of kernel.execute('create plot', 'python')) {
        outputs.push(output)
      }

      expect(outputs).toHaveLength(1)
      expect(outputs[0]).toMatchObject({
        type: 'display_data',
        data: { 'image/png': expect.any(String) },
      })
    })
  })

  describe('status management', () => {
    it('transitions through status states during execution', async () => {
      const statuses: KernelStatus[] = []
      kernel.onStatusChange((s) => statuses.push(s))

      for await (const output of kernel.execute('print("test")', 'python')) {
        void output // consume
      }

      expect(statuses).toContain('busy')
      expect(statuses[statuses.length - 1]).toBe('idle')
    })

    it('returns to idle after error', async () => {
      for await (const output of kernel.execute('error', 'python')) {
        void output // consume
      }

      expect(kernel.status).toBe('idle')
    })
  })

  describe('multiple executions', () => {
    it('handles sequential executions', async () => {
      const results: string[] = []

      for await (const output of kernel.execute('print("first")', 'python')) {
        if (output.type === 'stream') results.push(output.text)
      }

      for await (const output of kernel.execute('print("second")', 'python')) {
        if (output.type === 'stream') results.push(output.text)
      }

      expect(results).toEqual(['first\n', 'second\n'])
    })

    it('maintains idle status between executions', async () => {
      for await (const output of kernel.execute('1+1', 'python')) {
        void output // consume
      }

      expect(kernel.status).toBe('idle')

      for await (const output of kernel.execute('2+2', 'python')) {
        void output // consume
      }

      expect(kernel.status).toBe('idle')
    })
  })

  describe('connection management', () => {
    it('cannot execute when disconnected', async () => {
      await kernel.disconnect()

      // Create a new kernel to test disconnected state
      const newKernel = createTestKernel()

      await expect(async () => {
        for await (const output of newKernel.execute('1+1', 'python')) {
          void output // consume
        }
      }).rejects.toThrow()
    })

    it('can reconnect after disconnect', async () => {
      await kernel.disconnect()
      expect(kernel.status).toBe('disconnected')

      await kernel.connect({ type: 'test' } as KernelConfig)
      expect(kernel.status).toBe('idle')

      // Should be able to execute again
      const outputs: KernelOutput[] = []
      for await (const output of kernel.execute('1 + 1', 'python')) {
        outputs.push(output)
      }
      expect(outputs.length).toBeGreaterThan(0)
    })
  })

  describe('interrupt', () => {
    it('interrupt sets status to idle', async () => {
      await kernel.interrupt()
      expect(kernel.status).toBe('idle')
    })
  })
})

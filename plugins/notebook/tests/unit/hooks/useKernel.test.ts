/**
 * Unit tests for useKernel hook
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useKernel } from '@/hooks/useKernel'
import type { KernelPlugin, KernelConfig, KernelStatus, KernelOutput } from '@/kernels/types'

// Mock kernel implementation
function createMockKernel(id: string = 'pyodide'): KernelPlugin {
  let status: KernelStatus = 'disconnected'
  const statusCallbacks = new Set<(status: KernelStatus) => void>()

  return {
    id,
    name: 'Mock Kernel',
    languages: ['python'] as const,
    get status() {
      return status
    },
    async connect(/* config: KernelConfig */) {
      status = 'connecting'
      statusCallbacks.forEach((cb) => cb(status))
      await new Promise((resolve) => setTimeout(resolve, 10))
      status = 'idle'
      statusCallbacks.forEach((cb) => cb(status))
    },
    async disconnect() {
      status = 'disconnected'
      statusCallbacks.forEach((cb) => cb(status))
    },
    async *execute(/* code: string, language: 'python' | 'r' */): AsyncIterable<KernelOutput> {
      status = 'busy'
      statusCallbacks.forEach((cb) => cb(status))
      yield { type: 'stream', name: 'stdout', text: 'Hello' }
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

describe('useKernel', () => {
  let mockKernel: KernelPlugin

  beforeEach(() => {
    mockKernel = createMockKernel()
  })

  it('starts with disconnected status', () => {
    const { result } = renderHook(() => useKernel())
    expect(result.current.status).toBe('disconnected')
    expect(result.current.kernel).toBeNull()
  })

  it('connects to a kernel', async () => {
    const { result } = renderHook(() =>
      useKernel({ kernels: [mockKernel] })
    )

    await act(async () => {
      await result.current.connect({ type: 'pyodide' } as KernelConfig)
    })

    expect(result.current.kernel).toBe(mockKernel)
    expect(result.current.status).toBe('idle')
  })

  it('disconnects from kernel', async () => {
    const { result } = renderHook(() =>
      useKernel({ kernels: [mockKernel] })
    )

    await act(async () => {
      await result.current.connect({ type: 'pyodide' } as KernelConfig)
    })

    await act(async () => {
      await result.current.disconnect()
    })

    expect(result.current.status).toBe('disconnected')
  })

  it('throws when connecting to unknown kernel type', async () => {
    const { result } = renderHook(() =>
      useKernel({ kernels: [mockKernel] })
    )

    await expect(
      act(async () => {
        await result.current.connect({ type: 'unknown' } as KernelConfig)
      })
    ).rejects.toThrow('No kernel found for type: unknown')
  })

  it('calls onStatusChange callback', async () => {
    const onStatusChange = vi.fn()
    const { result } = renderHook(() =>
      useKernel({ kernels: [mockKernel], onStatusChange })
    )

    await act(async () => {
      await result.current.connect({ type: 'pyodide' } as KernelConfig)
    })

    expect(onStatusChange).toHaveBeenCalled()
  })

  it('can execute code', async () => {
    const { result } = renderHook(() =>
      useKernel({ kernels: [mockKernel] })
    )

    await act(async () => {
      await result.current.connect({ type: 'pyodide' } as KernelConfig)
    })

    const outputs: KernelOutput[] = []
    await act(async () => {
      for await (const output of result.current.execute('print("hello")', 'python')) {
        outputs.push(output)
      }
    })

    expect(outputs.length).toBeGreaterThan(0)
    expect(outputs[0].type).toBe('stream')
  })

  it('throws when executing without connection', () => {
    const { result } = renderHook(() => useKernel())

    expect(() => {
      result.current.execute('code', 'python')
    }).toThrow('No kernel connected')
  })
})

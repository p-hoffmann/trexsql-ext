/**
 * Unit tests for WebRKernel
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { WebRKernel } from '@/kernels/webr/WebRKernel'

// Mock the webr module
vi.mock('webr', () => {
  // Create a mock Shelter class that works as a constructor returning a promise
  class MockShelter {
    async captureR() {
      return {
        output: [{ type: 'stdout', data: 'Hello from R' }],
        images: [],
        result: null,
      }
    }
    purge() {
      // noop
    }
  }

  return {
    WebR: class MockWebR {
      async init() {
        return undefined
      }
      async close() {
        return undefined
      }
      interrupt() {
        // noop
      }
      async evalRVoid() {
        return undefined
      }
      // Shelter is used as: new webR.Shelter() which returns a Promise
      Shelter = class {
        constructor() {
          // Return a promise that resolves to a MockShelter instance
          return Promise.resolve(new MockShelter())
        }
      }
    },
  }
})

describe('WebRKernel', () => {
  let kernel: WebRKernel

  beforeEach(() => {
    kernel = new WebRKernel()
  })

  describe('properties', () => {
    it('has correct id', () => {
      expect(kernel.id).toBe('webr')
    })

    it('has correct name', () => {
      expect(kernel.name).toBe('R (WebR)')
    })

    it('supports R language', () => {
      expect(kernel.languages).toContain('r')
    })

    it('initial status is disconnected', () => {
      expect(kernel.status).toBe('disconnected')
    })
  })

  describe('connect', () => {
    it('connects successfully with valid config', async () => {
      await kernel.connect({ type: 'webr' })
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

      await kernel.connect({ type: 'webr' })

      expect(statusCallback).toHaveBeenCalledWith('connecting')
      expect(statusCallback).toHaveBeenCalledWith('idle')
    })

    it('installs preload packages if specified', async () => {
      await kernel.connect({
        type: 'webr',
        preloadPackages: ['ggplot2'],
      })
      expect(kernel.status).toBe('idle')
    })
  })

  describe('disconnect', () => {
    it('disconnects successfully', async () => {
      await kernel.connect({ type: 'webr' })
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
      await kernel.connect({ type: 'webr' })
    })

    it('executes R code and returns output', async () => {
      const outputs: unknown[] = []
      for await (const output of kernel.execute('print("Hello")', 'r')) {
        outputs.push(output)
      }

      expect(outputs.length).toBeGreaterThan(0)
      expect(outputs[0]).toHaveProperty('type', 'stream')
    })

    it('throws error for non-R language', async () => {
      await expect(async () => {
        for await (const output of kernel.execute('print("test")', 'python')) {
          void output // consume iterator
        }
      }).rejects.toThrow('WebRKernel only supports R')
    })

    it('throws error when not connected', async () => {
      await kernel.disconnect()

      await expect(async () => {
        for await (const output of kernel.execute('1+1', 'r')) {
          void output // consume iterator
        }
      }).rejects.toThrow('not connected')
    })

    it('sets status to busy during execution', async () => {
      const statusCallback = vi.fn()
      kernel.onStatusChange(statusCallback)

      for await (const output of kernel.execute('1+1', 'r')) {
        void output // consume iterator
      }

      expect(statusCallback).toHaveBeenCalledWith('busy')
      expect(statusCallback).toHaveBeenCalledWith('idle')
    })
  })

  describe('interrupt', () => {
    it('sets status to idle after interrupt', async () => {
      await kernel.connect({ type: 'webr' })
      await kernel.interrupt()
      expect(kernel.status).toBe('idle')
    })
  })

  describe('onStatusChange', () => {
    it('returns unsubscribe function', async () => {
      const callback = vi.fn()
      const unsubscribe = kernel.onStatusChange(callback)

      await kernel.connect({ type: 'webr' })
      expect(callback).toHaveBeenCalled()

      callback.mockClear()
      unsubscribe()

      await kernel.disconnect()
      expect(callback).not.toHaveBeenCalled()
    })
  })
})

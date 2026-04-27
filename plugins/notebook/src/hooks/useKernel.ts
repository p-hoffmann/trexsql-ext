import { useState, useCallback, useEffect, useRef, useMemo } from 'react'
import type { KernelPlugin, KernelConfig, KernelStatus, KernelOutput } from '@/kernels/types'
import type { KernelInfo } from '@/components/notebook/NotebookToolbar'

export interface UseKernelOptions {
  kernels?: KernelPlugin[]
  defaultConfig?: KernelConfig
  /** Configs for all kernels — each kernel is auto-connected with its matching config */
  kernelConfigs?: KernelConfig[]
  onStatusChange?: (status: KernelStatus) => void
}

export interface UseKernelReturn {
  kernel: KernelPlugin | null
  status: KernelStatus
  isConnecting: boolean
  availableKernels: KernelInfo[]
  activeKernelId: string | undefined
  connect: (config: KernelConfig) => Promise<void>
  disconnect: () => Promise<void>
  execute: (code: string, language: 'python' | 'r') => AsyncIterable<KernelOutput>
  interrupt: () => Promise<void>
  switchKernel: (kernelId: string) => Promise<void>
  /** Get the appropriate kernel for a given language */
  getKernelForLanguage: (language: 'python' | 'r') => KernelPlugin | null
  /** Aggregate status across all connected kernels */
  aggregateStatus: KernelStatus
  /** Per-kernel status map (kernel id → status) */
  kernelStatuses: Map<string, KernelStatus>
}

/** Compute a single status from multiple kernel statuses */
function computeAggregateStatus(statuses: KernelStatus[]): KernelStatus {
  if (statuses.length === 0) return 'disconnected'
  if (statuses.some((s) => s === 'error')) return 'error'
  if (statuses.some((s) => s === 'busy')) return 'busy'
  if (statuses.some((s) => s === 'connecting')) return 'connecting'
  if (statuses.every((s) => s === 'idle')) return 'idle'
  if (statuses.some((s) => s === 'idle')) return 'idle'
  return 'disconnected'
}

export function useKernel(options: UseKernelOptions = {}): UseKernelReturn {
  const { kernels = [], defaultConfig, kernelConfigs, onStatusChange } = options

  const [kernel, setKernel] = useState<KernelPlugin | null>(null)
  const [status, setStatus] = useState<KernelStatus>('disconnected')
  const [kernelStatuses, setKernelStatuses] = useState<Map<string, KernelStatus>>(new Map())
  const [isConnecting, setIsConnecting] = useState(false)
  const unsubscribeRef = useRef<(() => void) | null>(null)
  const unsubscribesRef = useRef<Map<string, () => void>>(new Map())
  const lastConfigRef = useRef<KernelConfig | null>(null)
  const multiKernelInitRef = useRef(false)
  const connectingRef = useRef(false)

  // Stabilize references to avoid re-triggering effects on every render
  const defaultConfigRef = useRef(defaultConfig)
  defaultConfigRef.current = defaultConfig
  const kernelsRef = useRef(kernels)
  kernelsRef.current = kernels

  const availableKernels = useMemo<KernelInfo[]>(
    () =>
      kernels.map((k) => ({
        id: k.id,
        name: k.name,
        languages: k.languages,
      })),
    [kernels]
  )

  const activeKernelId = kernel?.id

  const aggregateStatus = useMemo(() => {
    if (kernelStatuses.size === 0) return status
    return computeAggregateStatus(Array.from(kernelStatuses.values()))
  }, [kernelStatuses, status])

  const findKernel = useCallback(
    (config: KernelConfig): KernelPlugin | undefined => {
      return kernels.find((k) => k.id === config.type)
    },
    [kernels]
  )

  const getKernelForLanguage = useCallback(
    (language: 'python' | 'r'): KernelPlugin | null => {
      const k = kernels.find(
        (k) => k.languages.includes(language) && k.status !== 'disconnected'
      )
      return k ?? null
    },
    [kernels]
  )

  const connect = useCallback(
    async (config: KernelConfig) => {
      if (connectingRef.current) return
      connectingRef.current = true

      try {
        if (kernel && kernel.status !== 'disconnected') {
          await kernel.disconnect()
        }

        unsubscribeRef.current?.()

        const newKernel = findKernel(config)
        if (!newKernel) {
          throw new Error(`No kernel found for type: ${config.type}`)
        }

        setKernel(newKernel)
        setIsConnecting(true)
        lastConfigRef.current = config

        unsubscribeRef.current = newKernel.onStatusChange((newStatus) => {
          setStatus(newStatus)
          onStatusChange?.(newStatus)
        })

        await newKernel.connect(config)
        setStatus(newKernel.status)
      } catch (error) {
        setStatus('error')
        throw error
      } finally {
        setIsConnecting(false)
        connectingRef.current = false
      }
    },
    [kernel, findKernel, onStatusChange]
  )

  const disconnect = useCallback(async () => {
    if (kernel) {
      await kernel.disconnect()
      unsubscribeRef.current?.()
      setStatus('disconnected')
    }
  }, [kernel])

  const execute = useCallback(
    (code: string, language: 'python' | 'r'): AsyncIterable<KernelOutput> => {
      if (!kernel) {
        throw new Error('No kernel connected')
      }
      return kernel.execute(code, language)
    },
    [kernel]
  )

  const interrupt = useCallback(async () => {
    if (kernel) {
      await kernel.interrupt()
    }
  }, [kernel])

  const switchKernel = useCallback(
    async (kernelId: string) => {
      const targetKernel = kernels.find((k) => k.id === kernelId)
      if (!targetKernel) {
        throw new Error(`No kernel found with ID: ${kernelId}`)
      }

      const config: KernelConfig = { type: kernelId } as KernelConfig

      await connect(config)
    },
    [kernels, connect]
  )

  // Multi-kernel auto-connect: connect all kernels from kernelConfigs
  useEffect(() => {
    if (!kernelConfigs || kernelConfigs.length === 0 || kernels.length === 0) return
    if (multiKernelInitRef.current) return
    multiKernelInitRef.current = true

    const connectAll = async () => {
      const promises = kernelConfigs.map(async (config) => {
        const k = kernels.find((k) => k.id === config.type)
        if (!k || k.status !== 'disconnected') return

        const unsub = k.onStatusChange((newStatus) => {
          setKernelStatuses((prev) => {
            const next = new Map(prev)
            next.set(k.id, newStatus)
            return next
          })
        })
        unsubscribesRef.current.set(k.id, unsub)

        try {
          await k.connect(config)
          setKernelStatuses((prev) => {
            const next = new Map(prev)
            next.set(k.id, k.status)
            return next
          })
        } catch (e) {
          console.warn(`Failed to connect kernel ${k.id}:`, e)
          setKernelStatuses((prev) => {
            const next = new Map(prev)
            next.set(k.id, 'error')
            return next
          })
        }
      })
      await Promise.all(promises)
    }

    connectAll()
  }, [kernelConfigs, kernels])

  // Single-kernel auto-connect (backward compat): connect only the default kernel
  // Uses refs for defaultConfig/kernels to avoid re-firing on every render
  useEffect(() => {
    if (kernelConfigs && kernelConfigs.length > 0) return // multi-kernel mode
    const config = defaultConfigRef.current
    const availKernels = kernelsRef.current
    if (config && availKernels.length > 0 && !kernel && !connectingRef.current) {
      connect(config).catch(console.error)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kernel, connect, kernelConfigs])

  useEffect(() => {
    return () => {
      unsubscribeRef.current?.()
      for (const unsub of unsubscribesRef.current.values()) {
        unsub()
      }
      kernel?.disconnect()
    }
  }, [kernel])

  return {
    kernel,
    status,
    isConnecting,
    availableKernels,
    activeKernelId,
    connect,
    disconnect,
    execute,
    interrupt,
    switchKernel,
    getKernelForLanguage,
    aggregateStatus,
    kernelStatuses,
  }
}

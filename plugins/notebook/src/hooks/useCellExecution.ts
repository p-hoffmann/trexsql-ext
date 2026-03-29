import { useState, useCallback, useRef } from 'react'
import type { KernelPlugin, KernelOutput } from '@/kernels/types'
import { KernelInterruptError } from '@/kernels/types'
import type { CellId, CodeCellData, CellOutput } from '@/types/notebook'

export class NoKernelError extends Error {
  constructor() {
    super('No kernel connected. Please select a kernel to run code.')
    this.name = 'NoKernelError'
  }
}

export class ExecutionTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Execution timed out after ${Math.round(timeoutMs / 1000)} seconds`)
    this.name = 'ExecutionTimeoutError'
  }
}

export interface UseCellExecutionOptions {
  kernel: KernelPlugin | null
  /** Resolve the appropriate kernel for a given language (used for multi-kernel setups) */
  getKernelForLanguage?: (language: 'python' | 'r') => KernelPlugin | null
  executionTimeout?: number
  onCellExecutionStart?: (cellId: CellId) => void
  onCellExecutionEnd?: (cellId: CellId, success: boolean) => void
  onCellOutputAppend?: (cellId: CellId, output: CellOutput) => void
  onCellExecutionStateChange?: (cellId: CellId, state: CodeCellData['executionState']) => void
  onCellExecutionCountSet?: (cellId: CellId, count: number) => void
  onCellOutputsClear?: (cellId: CellId) => void
  onNoKernel?: (cellId: CellId) => void
}

export interface UseCellExecutionReturn {
  isExecuting: boolean
  executingCellId: CellId | null
  executionQueue: CellId[]
  executeCell: (
    cellId: CellId,
    code: string,
    language: 'python' | 'r'
  ) => Promise<void>
  executeCells: (
    cells: Array<{ id: CellId; code: string; language: 'python' | 'r' }>
  ) => Promise<void>
  interruptExecution: () => Promise<void>
}

let globalExecutionCount = 0
const DEFAULT_EXECUTION_TIMEOUT = 60_000

export function useCellExecution(
  options: UseCellExecutionOptions
): UseCellExecutionReturn {
  const {
    kernel,
    getKernelForLanguage,
    executionTimeout = DEFAULT_EXECUTION_TIMEOUT,
    onCellExecutionStart,
    onCellExecutionEnd,
    onCellOutputAppend,
    onCellExecutionStateChange,
    onCellExecutionCountSet,
    onCellOutputsClear,
    onNoKernel,
  } = options

  const [isExecuting, setIsExecuting] = useState(false)
  const [executingCellId, setExecutingCellId] = useState<CellId | null>(null)
  const [executionQueue, setExecutionQueue] = useState<CellId[]>([])
  const interruptedRef = useRef(false)

  const convertOutput = useCallback((output: KernelOutput, execCount: number): CellOutput | null => {
    switch (output.type) {
      case 'stream':
        return {
          type: 'stream',
          name: output.name,
          text: output.text,
        }
      case 'execute_result':
        return {
          type: 'execute_result',
          executionCount: execCount,
          data: output.data,
          metadata: output.metadata,
        }
      case 'display_data':
        return {
          type: 'display_data',
          data: output.data,
          metadata: output.metadata,
        }
      case 'error':
        return {
          type: 'error',
          ename: output.ename,
          evalue: output.evalue,
          traceback: output.traceback,
        }
      case 'status':
        return null
    }
  }, [])

  const executeCell = useCallback(
    async (cellId: CellId, code: string, language: 'python' | 'r') => {
      // Resolve the appropriate kernel for this language
      const targetKernel = getKernelForLanguage?.(language) ?? kernel
      if (!targetKernel) {
        onNoKernel?.(cellId)
        throw new NoKernelError()
      }

      if (targetKernel.status !== 'idle' && targetKernel.status !== 'busy') {
        throw new Error(`Kernel is not ready (status: ${targetKernel.status})`)
      }

      const executionCount = ++globalExecutionCount

      setIsExecuting(true)
      setExecutingCellId(cellId)
      interruptedRef.current = false

      onCellOutputsClear?.(cellId)
      onCellExecutionStateChange?.(cellId, 'running')
      onCellExecutionStart?.(cellId)

      let success = true
      let timeoutId: ReturnType<typeof setTimeout> | null = null

      try {
        const timeoutPromise = executionTimeout > 0
          ? new Promise<never>((_, reject) => {
              timeoutId = setTimeout(() => {
                interruptedRef.current = true
                targetKernel.interrupt().catch(() => {}) // Best effort interrupt
                reject(new ExecutionTimeoutError(executionTimeout))
              }, executionTimeout)
            })
          : null

        const executePromise = (async () => {
          for await (const output of targetKernel.execute(code, language)) {
            if (interruptedRef.current) {
              break
            }

            const cellOutput = convertOutput(output, executionCount)
            if (cellOutput) {
              onCellOutputAppend?.(cellId, cellOutput)

              if (cellOutput.type === 'error') {
                success = false
              }
            }
          }
        })()

        if (timeoutPromise) {
          await Promise.race([executePromise, timeoutPromise])
        } else {
          await executePromise
        }

        onCellExecutionCountSet?.(cellId, executionCount)
        onCellExecutionStateChange?.(cellId, success ? 'success' : 'error')
      } catch (error) {
        success = false

        if (error instanceof KernelInterruptError) {
          onCellExecutionStateChange?.(cellId, 'idle')
        } else if (error instanceof ExecutionTimeoutError) {
          const errorOutput: CellOutput = {
            type: 'error',
            ename: 'ExecutionTimeoutError',
            evalue: error.message,
            traceback: ['Execution was automatically cancelled due to timeout.'],
          }
          onCellOutputAppend?.(cellId, errorOutput)
          onCellExecutionStateChange?.(cellId, 'error')
        } else {
          const errorOutput: CellOutput = {
            type: 'error',
            ename: error instanceof Error ? error.constructor.name : 'Error',
            evalue: error instanceof Error ? error.message : String(error),
            traceback: error instanceof Error && error.stack ? error.stack.split('\n') : [],
          }
          onCellOutputAppend?.(cellId, errorOutput)
          onCellExecutionStateChange?.(cellId, 'error')
        }
      } finally {
        if (timeoutId) {
          clearTimeout(timeoutId)
        }
        setIsExecuting(false)
        setExecutingCellId(null)
        onCellExecutionEnd?.(cellId, success)
      }
    },
    [
      kernel,
      getKernelForLanguage,
      executionTimeout,
      convertOutput,
      onCellExecutionStart,
      onCellExecutionEnd,
      onCellOutputAppend,
      onCellExecutionStateChange,
      onCellExecutionCountSet,
      onCellOutputsClear,
      onNoKernel,
    ]
  )

  const executeCells = useCallback(
    async (cells: Array<{ id: CellId; code: string; language: 'python' | 'r' }>) => {
      setExecutionQueue(cells.map((c) => c.id))

      for (const cell of cells) {
        if (interruptedRef.current) {
          break
        }

        setExecutionQueue((queue) => queue.filter((id) => id !== cell.id))
        await executeCell(cell.id, cell.code, cell.language)
      }

      setExecutionQueue([])
    },
    [executeCell]
  )

  const interruptExecution = useCallback(async () => {
    interruptedRef.current = true
    setExecutionQueue([])

    if (kernel) {
      await kernel.interrupt()
    }

    if (executingCellId) {
      onCellExecutionStateChange?.(executingCellId, 'idle')
    }

    setIsExecuting(false)
    setExecutingCellId(null)
  }, [kernel, executingCellId, onCellExecutionStateChange])

  return {
    isExecuting,
    executingCellId,
    executionQueue,
    executeCell,
    executeCells,
    interruptExecution,
  }
}

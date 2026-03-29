import { useState, useCallback, useMemo } from 'react'
import type {
  CellId,
  CellLanguage,
  CellType,
  CodeCellData,
  NotebookData,
} from '@/types/notebook'
import { createCodeCell, createMarkdownCell, createEmptyNotebook, isCodeCell } from '@/types/notebook'
import type { KernelConfig, KernelStatus } from '@/kernels/types'

const MAX_HISTORY = 30

interface HistoryState {
  past: NotebookData[]
  future: NotebookData[]
}

export interface UseNotebookOptions {
  initialData?: NotebookData
  onChange?: (data: NotebookData) => void
}

export interface UseNotebookReturn {
  notebook: NotebookData
  selectedCellId: CellId | null
  kernelStatus: KernelStatus
  isExecuting: boolean
  actions: {
    addCell: (type: CellType, position?: number, language?: CellLanguage) => CellId
    deleteCell: (cellId: CellId) => void
    moveCell: (cellId: CellId, newPosition: number) => void
    updateCellSource: (cellId: CellId, source: string) => void
    selectCell: (cellId: CellId | null) => void
    runCell: (cellId: CellId) => Promise<void>
    runAllCells: () => Promise<void>
    interruptExecution: () => Promise<void>
    clearCellOutputs: (cellId: CellId) => void
    clearAllOutputs: () => void
    connectKernel: (config: KernelConfig) => Promise<void>
    disconnectKernel: () => Promise<void>
    undo: () => void
    redo: () => void
    setCellExecutionState: (cellId: CellId, state: CodeCellData['executionState']) => void
    appendCellOutput: (cellId: CellId, output: CodeCellData['outputs'][number]) => void
    setCellLanguage: (cellId: CellId, language: CellLanguage) => void
    setCellExecutionCount: (cellId: CellId, count: number | null) => void
    setNotebook: (notebook: NotebookData) => void
  }
  history: {
    canUndo: boolean
    canRedo: boolean
  }
}

export function useNotebook(options: UseNotebookOptions = {}): UseNotebookReturn {
  const { initialData, onChange } = options

  const [notebook, setNotebookState] = useState<NotebookData>(
    initialData ?? createEmptyNotebook()
  )
  const [selectedCellId, setSelectedCellId] = useState<CellId | null>(null)
  const [kernelStatus, setKernelStatus] = useState<KernelStatus>('disconnected')
  const [isExecuting] = useState(false)
  const [history, setHistory] = useState<HistoryState>({ past: [], future: [] })

  const updateNotebook = useCallback(
    (updater: (prev: NotebookData) => NotebookData, skipHistory = false) => {
      setNotebookState((prev) => {
        const next = updater(prev)

        if (!skipHistory) {
          setHistory((h) => ({
            past: [...h.past.slice(-MAX_HISTORY + 1), prev],
            future: [],
          }))
        }

        onChange?.(next)

        return next
      })
    },
    [onChange]
  )

  const addCell = useCallback(
    (type: CellType, position?: number, language: CellLanguage = 'python'): CellId => {
      const newCell = type === 'code' ? createCodeCell(language) : createMarkdownCell()

      updateNotebook((prev) => {
        const cells = [...prev.cells]
        const insertIndex = position ?? cells.length
        cells.splice(insertIndex, 0, newCell)
        return { ...prev, cells }
      })

      setSelectedCellId(newCell.id)
      return newCell.id
    },
    [updateNotebook]
  )

  const deleteCell = useCallback(
    (cellId: CellId) => {
      updateNotebook((prev) => {
        const index = prev.cells.findIndex((c) => c.id === cellId)
        if (index === -1) return prev

        const cells = prev.cells.filter((c) => c.id !== cellId)

        if (selectedCellId === cellId) {
          const nextIndex = Math.min(index, cells.length - 1)
          setSelectedCellId(cells[nextIndex]?.id ?? null)
        }

        return { ...prev, cells }
      })
    },
    [updateNotebook, selectedCellId]
  )

  const moveCell = useCallback(
    (cellId: CellId, newPosition: number) => {
      updateNotebook((prev) => {
        const index = prev.cells.findIndex((c) => c.id === cellId)
        if (index === -1) return prev

        const cells = [...prev.cells]
        const [cell] = cells.splice(index, 1)
        const targetIndex = Math.max(0, Math.min(newPosition, cells.length))
        cells.splice(targetIndex, 0, cell)

        return { ...prev, cells }
      })
    },
    [updateNotebook]
  )

  const updateCellSource = useCallback(
    (cellId: CellId, source: string) => {
      updateNotebook((prev) => ({
        ...prev,
        cells: prev.cells.map((cell) =>
          cell.id === cellId ? { ...cell, source } : cell
        ),
      }))
    },
    [updateNotebook]
  )

  const setCellLanguage = useCallback(
    (cellId: CellId, language: CellLanguage) => {
      updateNotebook((prev) => ({
        ...prev,
        cells: prev.cells.map((cell) =>
          cell.id === cellId && isCodeCell(cell) && cell.language !== language
            ? { ...cell, language, outputs: [], executionCount: null, executionState: 'idle' as const }
            : cell
        ),
      }))
    },
    [updateNotebook]
  )

  const selectCell = useCallback((cellId: CellId | null) => {
    setSelectedCellId(cellId)
  }, [])

  const setCellExecutionState = useCallback(
    (cellId: CellId, state: CodeCellData['executionState']) => {
      updateNotebook(
        (prev) => ({
          ...prev,
          cells: prev.cells.map((cell) =>
            cell.id === cellId && isCodeCell(cell)
              ? { ...cell, executionState: state }
              : cell
          ),
        }),
        true // Skip history for execution state changes
      )
    },
    [updateNotebook]
  )

  const appendCellOutput = useCallback(
    (cellId: CellId, output: CodeCellData['outputs'][number]) => {
      updateNotebook(
        (prev) => ({
          ...prev,
          cells: prev.cells.map((cell) =>
            cell.id === cellId && isCodeCell(cell)
              ? { ...cell, outputs: [...cell.outputs, output] }
              : cell
          ),
        }),
        true // Skip history for output changes
      )
    },
    [updateNotebook]
  )

  const setCellExecutionCount = useCallback(
    (cellId: CellId, count: number | null) => {
      updateNotebook(
        (prev) => ({
          ...prev,
          cells: prev.cells.map((cell) =>
            cell.id === cellId && isCodeCell(cell)
              ? { ...cell, executionCount: count }
              : cell
          ),
        }),
        true // Skip history
      )
    },
    [updateNotebook]
  )

  const clearCellOutputs = useCallback(
    (cellId: CellId) => {
      updateNotebook((prev) => ({
        ...prev,
        cells: prev.cells.map((cell) =>
          cell.id === cellId && isCodeCell(cell)
            ? { ...cell, outputs: [], executionCount: null, executionState: 'idle' }
            : cell
        ),
      }))
    },
    [updateNotebook]
  )

  const clearAllOutputs = useCallback(() => {
    updateNotebook((prev) => ({
      ...prev,
      cells: prev.cells.map((cell) =>
        isCodeCell(cell)
          ? { ...cell, outputs: [], executionCount: null, executionState: 'idle' }
          : cell
      ),
    }))
  }, [updateNotebook])

  // TODO: implement with useCellExecution hook
  const runCell = useCallback(async (/* cellId: CellId */) => {
    console.warn('runCell not yet implemented - needs kernel integration')
  }, [])

  const runAllCells = useCallback(async () => {
    console.warn('runAllCells not yet implemented - needs kernel integration')
  }, [])

  const interruptExecution = useCallback(async () => {
    console.warn('interruptExecution not yet implemented - needs kernel integration')
  }, [])

  // TODO: implement with useKernel hook
  const connectKernel = useCallback(async (/* config: KernelConfig */) => {
    setKernelStatus('connecting')
    console.warn('connectKernel not yet implemented')
  }, [])

  const disconnectKernel = useCallback(async () => {
    setKernelStatus('disconnected')
    console.warn('disconnectKernel not yet implemented')
  }, [])

  const undo = useCallback(() => {
    setHistory((h) => {
      if (h.past.length === 0) return h

      const previous = h.past[h.past.length - 1]
      const newPast = h.past.slice(0, -1)

      setNotebookState(() => {
        onChange?.(previous)
        return previous
      })

      return {
        past: newPast,
        future: [notebook, ...h.future],
      }
    })
  }, [notebook, onChange])

  const redo = useCallback(() => {
    setHistory((h) => {
      if (h.future.length === 0) return h

      const next = h.future[0]
      const newFuture = h.future.slice(1)

      setNotebookState(() => {
        onChange?.(next)
        return next
      })

      return {
        past: [...h.past, notebook],
        future: newFuture,
      }
    })
  }, [notebook, onChange])

  const setNotebook = useCallback(
    (newNotebook: NotebookData) => {
      updateNotebook(() => newNotebook)
    },
    [updateNotebook]
  )

  const actions = useMemo(
    () => ({
      addCell,
      deleteCell,
      moveCell,
      updateCellSource,
      setCellLanguage,
      selectCell,
      runCell,
      runAllCells,
      interruptExecution,
      clearCellOutputs,
      clearAllOutputs,
      connectKernel,
      disconnectKernel,
      undo,
      redo,
      setCellExecutionState,
      appendCellOutput,
      setCellExecutionCount,
      setNotebook,
    }),
    [
      addCell,
      deleteCell,
      moveCell,
      updateCellSource,
      setCellLanguage,
      selectCell,
      runCell,
      runAllCells,
      interruptExecution,
      clearCellOutputs,
      clearAllOutputs,
      connectKernel,
      disconnectKernel,
      undo,
      redo,
      setCellExecutionState,
      appendCellOutput,
      setCellExecutionCount,
      setNotebook,
    ]
  )

  const historyState = useMemo(
    () => ({
      canUndo: history.past.length > 0,
      canRedo: history.future.length > 0,
    }),
    [history.past.length, history.future.length]
  )

  return {
    notebook,
    selectedCellId,
    kernelStatus,
    isExecuting,
    actions,
    history: historyState,
  }
}

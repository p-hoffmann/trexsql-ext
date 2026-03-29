import { useCallback, forwardRef, useImperativeHandle, useMemo, useState, useEffect } from 'react'
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragStartEvent,
  DragOverlay,
} from '@dnd-kit/core'
import {
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable'
import { useNotebook, type UseNotebookOptions } from '@/hooks/useNotebook'
import { useKernel } from '@/hooks/useKernel'
import { useCellExecution } from '@/hooks/useCellExecution'
import { isCodeCell } from '@/types/notebook'
import { NotebookToolbar } from './NotebookToolbar'
import { Cell } from './Cell'
import { SortableCell } from './SortableCell'
import { Button } from '@/components/ui/button'
import { Plus, Code, FileText } from 'lucide-react'
import { cn } from '@/lib/utils'
import type {
  NotebookData,
  CellId,
  CellType,
  CellLanguage,
} from '@/types/notebook'
import type { KernelPlugin, KernelConfig, KernelStatus } from '@/kernels/types'

/** Threshold above which cells use content-visibility: auto for performance */
const VIRTUALIZATION_THRESHOLD = 50

/** Theme overrides applied as CSS custom properties on the notebook wrapper */
export interface NotebookTheme {
  /** Primary color (e.g. selected cells, primary buttons) */
  primary?: string
  /** Text color on primary backgrounds */
  primaryForeground?: string
  /** Main background color */
  background?: string
  /** Main text color */
  foreground?: string
  /** Secondary/hover background */
  secondary?: string
  /** Text on secondary backgrounds */
  secondaryForeground?: string
  /** Accent/active state background */
  accent?: string
  /** Text on accent backgrounds */
  accentForeground?: string
  /** Border color */
  border?: string
  /** Input border color */
  input?: string
  /** Focus ring color */
  ring?: string
  /** Muted/disabled color */
  muted?: string
  /** Muted text color */
  mutedForeground?: string
  /** Destructive/error color */
  destructive?: string
  /** Success color */
  success?: string
  /** Warning color */
  warning?: string
  /** Card background */
  card?: string
  /** Card text color */
  cardForeground?: string
}

export interface NotebookProps {
  initialData?: NotebookData
  data?: NotebookData
  onChange?: (data: NotebookData) => void
  kernels?: KernelPlugin[]
  defaultKernelConfig?: KernelConfig
  /** Configs for all kernels — each kernel is auto-connected with its matching config.
   *  When provided, cells are automatically routed to the correct kernel by language. */
  kernelConfigs?: KernelConfig[]
  onKernelStatusChange?: (status: KernelStatus) => void
  showToolbar?: boolean
  showLineNumbers?: boolean
  readOnly?: boolean
  className?: string
  /** Theme overrides — sets CSS custom properties on the notebook wrapper */
  theme?: NotebookTheme
  /** Whether to show the kernel selector dropdown in the toolbar (default: true) */
  showKernelSelector?: boolean
  onCellSelect?: (cellId: CellId | null) => void
  onCellExecuteStart?: (cellId: CellId) => void
  onCellExecuteEnd?: (cellId: CellId, success: boolean) => void
  /** Number of cells above which to enable content-visibility optimization (default: 50) */
  virtualizationThreshold?: number
}

export interface NotebookHandle {
  getNotebookData(): NotebookData
  setNotebookData(data: NotebookData): void
  addCell(type: CellType, position?: number, language?: CellLanguage): CellId
  deleteCell(cellId: CellId): void
  moveCell(cellId: CellId, newPosition: number): void
  focusCell(cellId: CellId): void
  getCellData(cellId: CellId): NotebookData['cells'][number] | undefined
  updateCellSource(cellId: CellId, source: string): void
  runCell(cellId: CellId): Promise<void>
  runAllCells(): Promise<void>
  runCellsBelow(cellId: CellId): Promise<void>
  interruptExecution(): Promise<void>
  clearCellOutputs(cellId: CellId): void
  clearAllOutputs(): void
  connectKernel(config: KernelConfig): Promise<void>
  disconnectKernel(): Promise<void>
  getKernelStatus(): KernelStatus
  undo(): void
  redo(): void
  canUndo(): boolean
  canRedo(): boolean
}

export const Notebook = forwardRef<NotebookHandle, NotebookProps>(function Notebook(
  props,
  ref
) {
  const {
    initialData,
    data,
    onChange,
    kernels,
    defaultKernelConfig,
    kernelConfigs,
    onKernelStatusChange,
    onCellExecuteStart,
    onCellExecuteEnd,
    showToolbar = true,
    showLineNumbers = true,
    readOnly = false,
    className,
    theme,
    showKernelSelector = true,
    onCellSelect,
    virtualizationThreshold = VIRTUALIZATION_THRESHOLD,
  } = props

  const themeStyle = useMemo(() => {
    if (!theme) return undefined
    const vars: Record<string, string> = {}
    const map: [keyof NotebookTheme, string][] = [
      ['primary', '--color-primary'],
      ['primaryForeground', '--color-primary-foreground'],
      ['background', '--color-background'],
      ['foreground', '--color-foreground'],
      ['secondary', '--color-secondary'],
      ['secondaryForeground', '--color-secondary-foreground'],
      ['accent', '--color-accent'],
      ['accentForeground', '--color-accent-foreground'],
      ['border', '--color-border'],
      ['input', '--color-input'],
      ['ring', '--color-ring'],
      ['muted', '--color-muted'],
      ['mutedForeground', '--color-muted-foreground'],
      ['destructive', '--color-destructive'],
      ['success', '--color-success'],
      ['warning', '--color-warning'],
      ['card', '--color-card'],
      ['cardForeground', '--color-card-foreground'],
    ]
    for (const [key, cssVar] of map) {
      if (theme[key]) vars[cssVar] = theme[key]
    }
    return vars
  }, [theme])

  const hookOptions: UseNotebookOptions = {
    initialData: data ?? initialData,
    onChange,
  }

  const {
    notebook,
    selectedCellId,
    actions,
    history,
  } = useNotebook(hookOptions)

  const {
    kernel,
    status: kernelStatus,
    aggregateStatus,
    availableKernels,
    activeKernelId,
    connect: connectKernel,
    disconnect: disconnectKernel,
    switchKernel,
    getKernelForLanguage,
    kernelStatuses,
  } = useKernel({
    kernels,
    defaultConfig: defaultKernelConfig,
    kernelConfigs,
    onStatusChange: onKernelStatusChange,
  })

  const cellExecution = useCellExecution({
    kernel,
    getKernelForLanguage,
    onCellExecutionStart: onCellExecuteStart,
    onCellExecutionEnd: onCellExecuteEnd,
    onCellOutputAppend: actions.appendCellOutput,
    onCellExecutionStateChange: actions.setCellExecutionState,
    onCellExecutionCountSet: actions.setCellExecutionCount,
    onCellOutputsClear: actions.clearCellOutputs,
  })

  const { isExecuting } = cellExecution
  // Use aggregate status when multiple kernels are connected
  const effectiveStatus = kernelConfigs ? aggregateStatus : kernelStatus
  const kernelReady = effectiveStatus === 'idle' || effectiveStatus === 'busy'

  const runCell = useCallback(
    async (cellId: CellId) => {
      const cell = notebook.cells.find((c) => c.id === cellId)
      if (!cell || !isCodeCell(cell)) return
      await cellExecution.executeCell(cellId, cell.source, cell.language)
    },
    [notebook.cells, cellExecution]
  )

  const runAllCells = useCallback(async () => {
    const codeCells = notebook.cells
      .filter(isCodeCell)
      .map((c) => ({ id: c.id, code: c.source, language: c.language }))
    await cellExecution.executeCells(codeCells)
  }, [notebook.cells, cellExecution])

  // Enable virtualization for large notebooks (uses CSS content-visibility: auto)
  const useVirtualization = notebook.cells.length >= virtualizationThreshold

  const [activeDragId, setActiveDragId] = useState<CellId | null>(null)

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8,
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  )

  const cellIds = useMemo(() => notebook.cells.map((c) => c.id), [notebook.cells])

  const activeDragCell = useMemo(
    () => notebook.cells.find((c) => c.id === activeDragId),
    [notebook.cells, activeDragId]
  )

  const handleDragStart = useCallback((event: DragStartEvent) => {
    setActiveDragId(event.active.id as CellId)
  }, [])

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event
      setActiveDragId(null)

      if (over && active.id !== over.id) {
        const oldIndex = notebook.cells.findIndex((c) => c.id === active.id)
        const newIndex = notebook.cells.findIndex((c) => c.id === over.id)

        if (oldIndex !== -1 && newIndex !== -1) {
          actions.moveCell(active.id as CellId, newIndex)
        }
      }
    },
    [notebook.cells, actions]
  )

  useImperativeHandle(ref, () => ({
    getNotebookData: () => notebook,
    setNotebookData: (newData) => actions.setNotebook(newData),
    addCell: actions.addCell,
    deleteCell: actions.deleteCell,
    moveCell: actions.moveCell,
    focusCell: (cellId) => actions.selectCell(cellId),
    getCellData: (cellId) => notebook.cells.find((c) => c.id === cellId),
    updateCellSource: actions.updateCellSource,
    runCell,
    runAllCells,
    runCellsBelow: async (cellId) => {
      const index = notebook.cells.findIndex((c) => c.id === cellId)
      if (index === -1) return
      const codeCells = notebook.cells
        .slice(index)
        .filter(isCodeCell)
        .map((c) => ({ id: c.id, code: c.source, language: c.language }))
      await cellExecution.executeCells(codeCells)
    },
    interruptExecution: cellExecution.interruptExecution,
    clearCellOutputs: actions.clearCellOutputs,
    clearAllOutputs: actions.clearAllOutputs,
    connectKernel,
    disconnectKernel,
    getKernelStatus: () => effectiveStatus,
    undo: actions.undo,
    redo: actions.redo,
    canUndo: () => history.canUndo,
    canRedo: () => history.canRedo,
  }))

  const handleSelectCell = useCallback(
    (cellId: CellId) => {
      actions.selectCell(cellId)
      onCellSelect?.(cellId)
    },
    [actions, onCellSelect]
  )

  const handleDeleteCell = useCallback(
    (cellId: CellId) => {
      actions.deleteCell(cellId)
    },
    [actions]
  )

  const handleMoveCell = useCallback(
    (cellId: CellId, direction: 'up' | 'down') => {
      const index = notebook.cells.findIndex((c) => c.id === cellId)
      if (index === -1) return

      const newIndex = direction === 'up' ? index - 1 : index + 1
      if (newIndex < 0 || newIndex >= notebook.cells.length) return

      actions.moveCell(cellId, newIndex)
    },
    [actions, notebook.cells]
  )

  const handleDuplicateCell = useCallback(
    (cellId: CellId) => {
      const cell = notebook.cells.find((c) => c.id === cellId)
      if (!cell) return

      const index = notebook.cells.findIndex((c) => c.id === cellId)
      const newCellId = actions.addCell(
        cell.type,
        index + 1,
        cell.type === 'code' ? cell.language : undefined
      )
      actions.updateCellSource(newCellId, cell.source)
    },
    [actions, notebook.cells]
  )

  const handleAddCodeCell = useCallback(
    (language: CellLanguage = 'python') => {
      actions.addCell('code', undefined, language)
    },
    [actions]
  )

  const handleAddMarkdownCell = useCallback(() => {
    actions.addCell('markdown')
  }, [actions])

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement
      if (
        target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.isContentEditable
      ) {
        if (event.key === 'Enter' && event.shiftKey && selectedCellId) {
          event.preventDefault()
          runCell(selectedCellId)
          return
        }
        if (event.key === 'Escape') {
          event.preventDefault()
          actions.selectCell(null)
          return
        }
        return
      }

      const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0
      const ctrlKey = isMac ? event.metaKey : event.ctrlKey

      if (ctrlKey && event.key === 'z' && !event.shiftKey) {
        event.preventDefault()
        actions.undo()
        return
      }

      if ((ctrlKey && event.key === 'z' && event.shiftKey) || (ctrlKey && event.key === 'y')) {
        event.preventDefault()
        actions.redo()
        return
      }

      if (event.key === 'Enter' && event.shiftKey && selectedCellId) {
        event.preventDefault()
        actions.runCell(selectedCellId)
        return
      }

      if (event.key === 'ArrowUp' && selectedCellId) {
        event.preventDefault()
        const index = notebook.cells.findIndex((c) => c.id === selectedCellId)
        if (index > 0) {
          actions.selectCell(notebook.cells[index - 1].id)
        }
        return
      }

      if (event.key === 'ArrowDown' && selectedCellId) {
        event.preventDefault()
        const index = notebook.cells.findIndex((c) => c.id === selectedCellId)
        if (index < notebook.cells.length - 1) {
          actions.selectCell(notebook.cells[index + 1].id)
        }
        return
      }

      if ((event.key === 'Delete' || event.key === 'Backspace') && selectedCellId) {
        event.preventDefault()
        handleDeleteCell(selectedCellId)
        return
      }

      if (event.key === 'Escape') {
        event.preventDefault()
        actions.selectCell(null)
        return
      }

      if (event.key === 'a' && selectedCellId) {
        event.preventDefault()
        const index = notebook.cells.findIndex((c) => c.id === selectedCellId)
        const selectedCell = notebook.cells[index]
        const lang: CellLanguage = selectedCell && isCodeCell(selectedCell) ? selectedCell.language : 'python'
        actions.addCell('code', index, lang)
        return
      }

      if (event.key === 'b' && selectedCellId) {
        event.preventDefault()
        const index = notebook.cells.findIndex((c) => c.id === selectedCellId)
        const selectedCell = notebook.cells[index]
        const lang: CellLanguage = selectedCell && isCodeCell(selectedCell) ? selectedCell.language : 'python'
        actions.addCell('code', index + 1, lang)
        return
      }

      if (event.key === 'm' && selectedCellId) {
        event.preventDefault()
        const cell = notebook.cells.find((c) => c.id === selectedCellId)
        if (cell && cell.type === 'code') {
          const index = notebook.cells.findIndex((c) => c.id === selectedCellId)
          actions.deleteCell(selectedCellId)
          const newId = actions.addCell('markdown', index)
          actions.updateCellSource(newId, cell.source)
        }
        return
      }

      if (event.key === 'y' && selectedCellId && !ctrlKey) {
        event.preventDefault()
        const cell = notebook.cells.find((c) => c.id === selectedCellId)
        if (cell && cell.type === 'markdown') {
          const index = notebook.cells.findIndex((c) => c.id === selectedCellId)
          actions.deleteCell(selectedCellId)
          const newId = actions.addCell('code', index, 'python')
          actions.updateCellSource(newId, cell.source)
        }
        return
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [actions, selectedCellId, notebook.cells, handleDeleteCell, runCell])

  return (
    <div className={cn('flex flex-col gap-4', className)} style={themeStyle}>
      {showToolbar && (
        <NotebookToolbar
          kernelStatus={effectiveStatus}
          isExecuting={isExecuting}
          canUndo={history.canUndo}
          canRedo={history.canRedo}
          availableKernels={availableKernels}
          activeKernelId={activeKernelId}
          showKernelSelector={showKernelSelector}
          kernelStatuses={kernelStatuses}
          onKernelChange={switchKernel}
          onAddCodeCell={handleAddCodeCell}
          onAddMarkdownCell={handleAddMarkdownCell}
          onRunAllCells={runAllCells}
          onInterruptExecution={cellExecution.interruptExecution}
          onUndo={actions.undo}
          onRedo={actions.redo}
        />
      )}

      <div className="flex flex-col gap-2">
        {notebook.cells.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-4 rounded-lg border-2 border-dashed border-muted py-12">
            <p className="text-muted-foreground">Add your first cell</p>
            <div className="flex gap-2">
              <Button
                variant="outline"
                onClick={() => handleAddCodeCell('python')}
                className="gap-2"
              >
                <Code className="h-4 w-4" />
                Python
              </Button>
              <Button
                variant="outline"
                onClick={() => handleAddCodeCell('r')}
                className="gap-2"
              >
                <Code className="h-4 w-4" />
                R
              </Button>
              <Button
                variant="outline"
                onClick={handleAddMarkdownCell}
                className="gap-2"
              >
                <FileText className="h-4 w-4" />
                Markdown
              </Button>
            </div>
          </div>
        ) : (
          <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragStart={handleDragStart}
            onDragEnd={handleDragEnd}
          >
            <SortableContext items={cellIds} strategy={verticalListSortingStrategy}>
              {notebook.cells.map((cell, index) => (
                <SortableCell
                  key={cell.id}
                  id={cell.id}
                  cell={cell}
                  isSelected={selectedCellId === cell.id}
                  showLineNumbers={showLineNumbers}
                  readOnly={readOnly}
                  kernelReady={kernelReady}
                  useVirtualization={useVirtualization}
                  onSelect={() => handleSelectCell(cell.id)}
                  onUpdateSource={(source) => actions.updateCellSource(cell.id, source)}
                  onRun={() => runCell(cell.id)}
                  onDelete={() => handleDeleteCell(cell.id)}
                  onMoveUp={() => handleMoveCell(cell.id, 'up')}
                  onMoveDown={() => handleMoveCell(cell.id, 'down')}
                  onDuplicate={() => handleDuplicateCell(cell.id)}
                  onChangeLanguage={(lang) => actions.setCellLanguage(cell.id, lang)}
                  canMoveUp={index > 0}
                  canMoveDown={index < notebook.cells.length - 1}
                />
              ))}
            </SortableContext>

            <DragOverlay>
              {activeDragCell ? (
                <div className="opacity-80 shadow-lg">
                  <Cell
                    cell={activeDragCell}
                    isSelected={false}
                    showLineNumbers={showLineNumbers}
                    readOnly={true}
                  />
                </div>
              ) : null}
            </DragOverlay>

            {!readOnly && (
              <div className="flex justify-center gap-2 py-4">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => handleAddCodeCell('python')}
                  className="gap-1 text-muted-foreground hover:text-foreground"
                >
                  <Plus className="h-4 w-4" />
                  Python
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => handleAddCodeCell('r')}
                  className="gap-1 text-muted-foreground hover:text-foreground"
                >
                  <Plus className="h-4 w-4" />
                  R
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleAddMarkdownCell}
                  className="gap-1 text-muted-foreground hover:text-foreground"
                >
                  <Plus className="h-4 w-4" />
                  Markdown
                </Button>
              </div>
            )}
          </DndContext>
        )}
      </div>
    </div>
  )
})

import React from 'react'
import { Plus, Code, FileText, Play, Square, Undo2, Redo2, ChevronDown } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from '@/components/ui/dropdown-menu'
import { Separator } from '@/components/ui/separator'
import { cn } from '@/lib/utils'
import type { KernelStatus } from '@/kernels/types'
import type { CellLanguage } from '@/types/notebook'

export interface KernelInfo {
  id: string
  name: string
  languages: ReadonlyArray<'python' | 'r'>
}

export interface NotebookToolbarProps {
  kernelStatus: KernelStatus
  isExecuting: boolean
  canUndo: boolean
  canRedo: boolean
  activeKernelId?: string
  availableKernels?: KernelInfo[]
  showKernelSelector?: boolean
  /** Per-kernel status map (kernel id → status) for multi-kernel display */
  kernelStatuses?: Map<string, KernelStatus>
  onAddCodeCell?: (language?: CellLanguage) => void
  onAddMarkdownCell?: () => void
  onRunAllCells?: () => void
  onInterruptExecution?: () => void
  onUndo?: () => void
  onRedo?: () => void
  onKernelChange?: (kernelId: string) => void
  className?: string
}

/** Display label for a kernel status */
function statusLabel(status: KernelStatus): string {
  if (status === 'connecting') return 'loading'
  return status
}

export function NotebookToolbar({
  kernelStatus,
  isExecuting,
  canUndo,
  canRedo,
  activeKernelId,
  availableKernels = [],
  showKernelSelector = true,
  kernelStatuses,
  onAddCodeCell,
  onAddMarkdownCell,
  onRunAllCells,
  onInterruptExecution,
  onUndo,
  onRedo,
  onKernelChange,
  className,
}: NotebookToolbarProps) {
  const activeKernel = availableKernels.find((k) => k.id === activeKernelId)
  const hasPerKernelStatuses = kernelStatuses && kernelStatuses.size > 0
  return (
    <div
      className={cn(
        'flex items-center gap-1 rounded-lg border bg-background p-1',
        className
      )}
    >
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="sm" className="gap-1">
            <Plus className="h-4 w-4" />
            Add Cell
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuItem onClick={() => onAddCodeCell?.('python')}>
            <Code className="mr-2 h-4 w-4" />
            Python Cell
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => onAddCodeCell?.('r')}>
            <Code className="mr-2 h-4 w-4" />
            R Cell
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => onAddMarkdownCell?.()}>
            <FileText className="mr-2 h-4 w-4" />
            Markdown Cell
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <Separator orientation="vertical" className="mx-1 h-6" />

      {isExecuting ? (
        <Button
          variant="ghost"
          size="sm"
          onClick={onInterruptExecution}
          className="gap-1 text-destructive hover:text-destructive"
        >
          <Square className="h-4 w-4" />
          Stop
        </Button>
      ) : (
        <Button
          variant="ghost"
          size="sm"
          onClick={onRunAllCells}
          disabled={kernelStatus !== 'idle'}
          className="gap-1"
        >
          <Play className="h-4 w-4" />
          Run All
        </Button>
      )}

      <Separator orientation="vertical" className="mx-1 h-6" />

      <Button
        variant="ghost"
        size="icon"
        className="h-8 w-8"
        onClick={onUndo}
        disabled={!canUndo}
        title="Undo (Ctrl+Z)"
      >
        <Undo2 className="h-4 w-4" />
      </Button>
      <Button
        variant="ghost"
        size="icon"
        className="h-8 w-8"
        onClick={onRedo}
        disabled={!canRedo}
        title="Redo (Ctrl+Shift+Z)"
      >
        <Redo2 className="h-4 w-4" />
      </Button>

      <div className="ml-auto flex items-center gap-3 px-2 text-sm">
        {showKernelSelector && availableKernels.length > 0 && (
          <>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="ghost"
                  size="sm"
                  className="gap-1 text-muted-foreground"
                  disabled={kernelStatus === 'busy' || isExecuting}
                >
                  {activeKernel?.name || 'Select Kernel'}
                  <ChevronDown className="h-3 w-3" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {availableKernels.map((kernel, index) => (
                  <React.Fragment key={kernel.id}>
                    <DropdownMenuItem
                      onClick={() => onKernelChange?.(kernel.id)}
                      className={cn(
                        kernel.id === activeKernelId && 'bg-accent'
                      )}
                    >
                      <div className="flex flex-col">
                        <span>{kernel.name}</span>
                        <span className="text-xs text-muted-foreground">
                          {kernel.languages.join(', ')}
                        </span>
                      </div>
                    </DropdownMenuItem>
                    {index < availableKernels.length - 1 && (
                      <DropdownMenuSeparator />
                    )}
                  </React.Fragment>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>
            <Separator orientation="vertical" className="h-6" />
          </>
        )}
        {hasPerKernelStatuses ? (
          availableKernels.map((k, i) => {
            const st = kernelStatuses.get(k.id) ?? 'disconnected'
            return (
              <React.Fragment key={k.id}>
                {i > 0 && <Separator orientation="vertical" className="h-4" />}
                <div className="flex items-center gap-1.5">
                  <div
                    className={cn(
                      'h-2 w-2 shrink-0 rounded-full',
                      st === 'idle' && 'bg-success',
                      st === 'busy' && 'bg-warning animate-pulse',
                      st === 'connecting' && 'bg-warning animate-pulse',
                      st === 'error' && 'bg-destructive',
                      st === 'disconnected' && 'bg-muted'
                    )}
                  />
                  <span className="whitespace-nowrap text-muted-foreground">
                    {k.languages.includes('r') ? 'R' : 'Python'}
                  </span>
                </div>
              </React.Fragment>
            )
          })
        ) : availableKernels.length > 0 ? (
          <>
            <div
              className={cn(
                'h-2 w-2 rounded-full',
                kernelStatus === 'idle' && 'bg-success',
                kernelStatus === 'busy' && 'bg-warning animate-pulse',
                kernelStatus === 'connecting' && 'bg-warning animate-pulse',
                kernelStatus === 'error' && 'bg-destructive',
                kernelStatus === 'disconnected' && 'bg-muted'
              )}
            />
            <span className="capitalize text-muted-foreground">{statusLabel(kernelStatus)}</span>
          </>
        ) : null}
      </div>
    </div>
  )
}

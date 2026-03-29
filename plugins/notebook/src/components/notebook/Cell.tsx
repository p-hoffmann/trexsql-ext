import { useCallback } from 'react'
import { MoreHorizontal, Play, Trash2, ChevronUp, ChevronDown, Copy } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { CodeCell } from './CodeCell'
import { MarkdownCell } from './MarkdownCell'
import { CellOutput } from './CellOutput'
import { cn } from '@/lib/utils'
import type { CellData, CellLanguage } from '@/types/notebook'
import { isCodeCell } from '@/types/notebook'

export interface CellProps {
  cell: CellData
  isSelected: boolean
  showLineNumbers?: boolean
  readOnly?: boolean
  /** Whether the kernel is ready to execute code (idle or busy) */
  kernelReady?: boolean
  onSelect?: () => void
  onUpdateSource?: (source: string) => void
  onRun?: () => void
  onDelete?: () => void
  onMoveUp?: () => void
  onMoveDown?: () => void
  onDuplicate?: () => void
  onChangeLanguage?: (language: CellLanguage) => void
  canMoveUp?: boolean
  canMoveDown?: boolean
}

export function Cell({
  cell,
  isSelected,
  showLineNumbers = true,
  readOnly = false,
  kernelReady = true,
  onSelect,
  onUpdateSource,
  onRun,
  onDelete,
  onMoveUp,
  onMoveDown,
  onDuplicate,
  onChangeLanguage,
  canMoveUp = true,
  canMoveDown = true,
}: CellProps) {
  const handleChange = useCallback(
    (value: string) => {
      onUpdateSource?.(value)
    },
    [onUpdateSource]
  )

  const handleFocus = useCallback(() => {
    onSelect?.()
  }, [onSelect])

  return (
    <div
      className={cn(
        'group relative',
        isSelected && 'z-10'
      )}
      onClick={handleFocus}
    >
      {/* Cell type label — always visible */}
      <span className="absolute -top-2 right-2 z-20 rounded-md bg-background/95 px-2 py-0.5 text-xs font-medium text-primary shadow-sm ring-1 ring-border/50 select-none">
        {isCodeCell(cell)
          ? cell.language === 'r' ? 'R' : 'Python'
          : 'Markdown'}
      </span>

      {/* Action buttons — visible on hover/select */}
      <div
        className={cn(
          'absolute -top-2 right-2 z-30 flex items-center gap-1 rounded-md bg-background/95 p-1 shadow-sm ring-1 ring-border/50 opacity-0 transition-opacity',
          (isSelected || 'group-hover:opacity-100'),
          isSelected && 'opacity-100'
        )}
      >
        {isCodeCell(cell) ? (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 px-1.5 text-xs font-medium text-primary select-none"
                onClick={(e) => e.stopPropagation()}
              >
                {cell.language === 'r' ? 'R' : 'Python'}
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start">
              <DropdownMenuItem onClick={() => onChangeLanguage?.('python')}>
                Python
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => onChangeLanguage?.('r')}>
                R
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        ) : (
          <span className="px-1.5 text-xs font-medium text-primary select-none">
            Markdown
          </span>
        )}

        {isCodeCell(cell) && (
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7"
            onClick={(e) => {
              e.stopPropagation()
              onRun?.()
            }}
            disabled={readOnly || !kernelReady}
            title="Run cell (Shift+Enter)"
          >
            <Play className="h-4 w-4" />
          </Button>
        )}

        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7"
          onClick={(e) => {
            e.stopPropagation()
            onMoveUp?.()
          }}
          disabled={!canMoveUp || readOnly}
          title="Move up"
        >
          <ChevronUp className="h-4 w-4" />
        </Button>

        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7"
          onClick={(e) => {
            e.stopPropagation()
            onMoveDown?.()
          }}
          disabled={!canMoveDown || readOnly}
          title="Move down"
        >
          <ChevronDown className="h-4 w-4" />
        </Button>

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={(e) => e.stopPropagation()}
            >
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {isCodeCell(cell) && (
              <>
                <DropdownMenuItem onClick={() => onRun?.()}>
                  <Play className="mr-2 h-4 w-4" />
                  Run cell
                </DropdownMenuItem>
                <DropdownMenuSeparator />
              </>
            )}
            <DropdownMenuItem onClick={() => onDuplicate?.()}>
              <Copy className="mr-2 h-4 w-4" />
              Duplicate
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onClick={() => onDelete?.()}
              className="text-destructive focus:text-destructive"
            >
              <Trash2 className="mr-2 h-4 w-4" />
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="py-2">
        {isCodeCell(cell) ? (
          <>
            <CodeCell
              source={cell.source}
              language={cell.language}
              executionCount={cell.executionCount}
              executionState={cell.executionState}
              isSelected={isSelected}
              readOnly={readOnly}
              showLineNumbers={showLineNumbers}
              onChange={handleChange}
              onFocus={handleFocus}
            />
            {cell.outputs.length > 0 && (
              <CellOutput outputs={cell.outputs} />
            )}
          </>
        ) : (
          <MarkdownCell
            source={cell.source}
            isSelected={isSelected}
            readOnly={readOnly}
            showLineNumbers={showLineNumbers}
            onChange={handleChange}
            onFocus={handleFocus}
          />
        )}
      </div>
    </div>
  )
}

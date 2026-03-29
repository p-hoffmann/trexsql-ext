import { useCallback, useMemo } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { StreamLanguage } from '@codemirror/language'
import { r } from '@codemirror/legacy-modes/mode/r'
import { githubLight } from '@uiw/codemirror-theme-github'
import { cn } from '@/lib/utils'
import type { CellLanguage, ExecutionState } from '@/types/notebook'

export interface CodeCellProps {
  source: string
  language: CellLanguage
  executionCount: number | null
  executionState: ExecutionState
  isSelected: boolean
  readOnly?: boolean
  showLineNumbers?: boolean
  onChange?: (value: string) => void
  onFocus?: () => void
}

const rLanguage = StreamLanguage.define(r)

export function CodeCell({
  source,
  language,
  executionCount,
  executionState,
  isSelected,
  readOnly = false,
  showLineNumbers = true,
  onChange,
  onFocus,
}: CodeCellProps) {
  const extensions = useMemo(() => {
    const langExtension = language === 'python' ? python() : rLanguage
    return [langExtension]
  }, [language])

  const handleChange = useCallback(
    (value: string) => {
      onChange?.(value)
    },
    [onChange]
  )

  const executionLabel = useMemo(() => {
    if (executionCount !== null) {
      return `[${executionCount}]`
    }
    if (executionState === 'running') {
      return '[*]'
    }
    return '[ ]'
  }, [executionCount, executionState])

  return (
    <div
      className={cn(
        'group relative flex border-l-2 transition-colors',
        isSelected ? 'border-primary' : 'border-transparent',
        executionState === 'running' && 'bg-accent/30'
      )}
    >
      <div className="flex w-16 shrink-0 items-start justify-end pr-2 pt-2 font-mono text-xs text-muted-foreground">
        {executionLabel}
      </div>

      <div className="min-w-0 flex-1 overflow-hidden">
        <CodeMirror
          value={source}
          extensions={extensions}
          onChange={handleChange}
          onFocus={onFocus}
          editable={!readOnly}
          basicSetup={{
            lineNumbers: showLineNumbers,
            highlightActiveLineGutter: true,
            highlightSpecialChars: true,
            history: true,
            foldGutter: true,
            drawSelection: true,
            dropCursor: true,
            allowMultipleSelections: true,
            indentOnInput: true,
            syntaxHighlighting: true,
            bracketMatching: true,
            closeBrackets: true,
            autocompletion: true,
            rectangularSelection: true,
            crosshairCursor: true,
            highlightActiveLine: true,
            highlightSelectionMatches: true,
            closeBracketsKeymap: true,
            defaultKeymap: true,
            searchKeymap: true,
            historyKeymap: true,
            foldKeymap: true,
            completionKeymap: true,
            lintKeymap: true,
            tabSize: 4,
          }}
          theme={githubLight}
          className={cn(
            'overflow-hidden rounded-md border bg-background text-sm',
            isSelected && 'ring-1 ring-ring'
          )}
          style={{
            fontSize: '14px',
          }}
        />
      </div>

      {executionState === 'running' && (
        <div className="absolute right-2 top-2">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-primary border-t-transparent" />
        </div>
      )}
    </div>
  )
}

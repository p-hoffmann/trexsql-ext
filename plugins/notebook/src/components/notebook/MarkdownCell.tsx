import { useState, useCallback, useMemo } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { markdown } from '@codemirror/lang-markdown'
import { marked } from 'marked'
import DOMPurify from 'dompurify'
import { githubLight } from '@uiw/codemirror-theme-github'
import { cn } from '@/lib/utils'

export interface MarkdownCellProps {
  source: string
  isSelected: boolean
  readOnly?: boolean
  showLineNumbers?: boolean
  onChange?: (value: string) => void
  onFocus?: () => void
}

export function MarkdownCell({
  source,
  isSelected,
  readOnly = false,
  showLineNumbers = false,
  onChange,
  onFocus,
}: MarkdownCellProps) {
  const [isEditing, setIsEditing] = useState(false)

  const renderedHtml = useMemo(() => {
    if (!source.trim()) {
      return '<p class="text-muted-foreground italic">Click to add markdown content...</p>'
    }

    try {
      const rawHtml = marked.parse(source, { async: false }) as string
      return DOMPurify.sanitize(rawHtml)
    } catch {
      return `<pre>${source}</pre>`
    }
  }, [source])

  const extensions = useMemo(() => [markdown()], [])

  const handleChange = useCallback(
    (value: string) => {
      onChange?.(value)
    },
    [onChange]
  )

  const handleDoubleClick = useCallback(() => {
    if (!readOnly) {
      setIsEditing(true)
    }
  }, [readOnly])

  const handleBlur = useCallback(() => {
    setIsEditing(false)
  }, [])

  const handleFocus = useCallback(() => {
    onFocus?.()
  }, [onFocus])

  const showEditMode = isEditing && isSelected

  if (showEditMode) {
    return (
      <div
        className={cn(
          'group relative border-l-2 transition-colors',
          isSelected ? 'border-primary' : 'border-transparent'
        )}
      >
        <div className="ml-16">
          <CodeMirror
            value={source}
            extensions={extensions}
            onChange={handleChange}
            onFocus={handleFocus}
            onBlur={handleBlur}
            autoFocus
            basicSetup={{
              lineNumbers: showLineNumbers,
              highlightActiveLineGutter: true,
              highlightActiveLine: true,
              foldGutter: false,
              dropCursor: true,
              allowMultipleSelections: true,
              indentOnInput: true,
              syntaxHighlighting: true,
              bracketMatching: true,
              closeBrackets: true,
              autocompletion: false,
              tabSize: 2,
            }}
            theme={githubLight}
            className={cn(
              'overflow-hidden rounded-md border bg-background text-sm',
              isSelected && 'ring-1 ring-ring'
            )}
            style={{ fontSize: '14px' }}
          />
        </div>
      </div>
    )
  }

  return (
    <div
      className={cn(
        'group relative border-l-2 transition-colors',
        isSelected ? 'border-primary' : 'border-transparent'
      )}
      onDoubleClick={handleDoubleClick}
      onClick={handleFocus}
    >
      <div
        className={cn(
          'ml-16 min-h-[40px] rounded-md px-4 py-2 cursor-text',
          isSelected && 'bg-accent/30 ring-1 ring-ring'
        )}
      >
        <div
          className="prose prose-sm max-w-none dark:prose-invert prose-headings:my-2 prose-p:my-2 prose-pre:my-2 prose-ul:my-2 prose-ol:my-2"
          dangerouslySetInnerHTML={{ __html: renderedHtml }}
        />
      </div>
    </div>
  )
}

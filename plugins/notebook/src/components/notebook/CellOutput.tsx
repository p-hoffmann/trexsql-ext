import { useState, useMemo } from 'react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { ChevronDown, ChevronUp } from 'lucide-react'
import type { CellOutput as CellOutputType, MimeBundle } from '@/types/notebook'

const MAX_OUTPUT_SIZE = 500 * 1024 // 500KB truncation threshold
const PREVIEW_SIZE = 10 * 1024

export interface CellOutputProps {
  outputs: CellOutputType[]
  className?: string
}

const MIME_PRIORITY = [
  'text/html',
  'image/svg+xml',
  'image/png',
  'image/jpeg',
  'text/markdown',
  'text/plain',
]

function getBestMimeType(data: MimeBundle): string {
  for (const mimeType of MIME_PRIORITY) {
    if (data[mimeType] !== undefined) {
      return mimeType
    }
  }
  return 'text/plain'
}

interface OutputItemProps {
  output: CellOutputType
}

interface TruncatableTextProps {
  text: string
  className?: string
}

function TruncatableText({ text, className }: TruncatableTextProps) {
  const [isExpanded, setIsExpanded] = useState(false)
  const isTruncated = text.length > MAX_OUTPUT_SIZE

  const displayText = useMemo(() => {
    if (!isTruncated || isExpanded) {
      return text
    }
    return text.slice(0, PREVIEW_SIZE) + '\n...'
  }, [text, isTruncated, isExpanded])

  return (
    <div>
      <pre className={cn('whitespace-pre-wrap font-mono text-sm', className)}>
        {displayText}
      </pre>
      {isTruncated && (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setIsExpanded(!isExpanded)}
          className="mt-1 text-muted-foreground"
        >
          {isExpanded ? (
            <>
              <ChevronUp className="mr-1 h-3 w-3" />
              Show less
            </>
          ) : (
            <>
              <ChevronDown className="mr-1 h-3 w-3" />
              Show more ({(text.length / 1024).toFixed(1)} KB)
            </>
          )}
        </Button>
      )}
    </div>
  )
}

function StreamOutput({ output }: { output: Extract<CellOutputType, { type: 'stream' }> }) {
  const isStderr = output.name === 'stderr'
  return (
    <TruncatableText
      text={output.text}
      className={isStderr ? 'text-destructive' : undefined}
    />
  )
}

function ErrorOutput({ output }: { output: Extract<CellOutputType, { type: 'error' }> }) {
  return (
    <div className="font-mono text-sm text-destructive">
      <div className="font-bold">
        {output.ename}: {output.evalue}
      </div>
      {output.traceback.length > 0 && (
        <pre className="mt-2 whitespace-pre-wrap text-xs opacity-80">
          {output.traceback.join('\n')}
        </pre>
      )}
    </div>
  )
}

function MimeOutput({ data }: { data: MimeBundle }) {
  const mimeType = getBestMimeType(data)
  const content = data[mimeType]

  if (content === undefined || content === null) {
    return null
  }

  switch (mimeType) {
    case 'text/html':
      return (
        <div
          className="prose prose-sm max-w-none dark:prose-invert"
          dangerouslySetInnerHTML={{ __html: String(content) }}
        />
      )

    case 'image/png':
    case 'image/jpeg':
      return (
        <img
          src={
            String(content).startsWith('data:')
              ? String(content)
              : `data:${mimeType};base64,${content}`
          }
          alt="Output"
          className="max-w-full"
        />
      )

    case 'image/svg+xml':
      return (
        <div
          className="max-w-full"
          dangerouslySetInnerHTML={{ __html: String(content) }}
        />
      )

    case 'text/markdown':
      return <TruncatableText text={String(content)} />

    case 'text/plain':
    default:
      return <TruncatableText text={String(content)} />
  }
}

function OutputItem({ output }: OutputItemProps) {
  switch (output.type) {
    case 'stream':
      return <StreamOutput output={output} />

    case 'error':
      return <ErrorOutput output={output} />

    case 'execute_result':
    case 'display_data':
      return <MimeOutput data={output.data} />

    default:
      return null
  }
}

export function CellOutput({
  outputs,
  className,
}: CellOutputProps) {
  if (outputs.length === 0) {
    return null
  }

  return (
    <div
      className={cn(
        'ml-16 pl-4 py-2',
        className
      )}
    >
      {outputs.map((output, index) => (
        <div key={index} className="mb-2 last:mb-0">
          <OutputItem output={output} />
        </div>
      ))}
    </div>
  )
}

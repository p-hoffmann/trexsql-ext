import { useState, useCallback, useMemo, type FC } from 'react'
import { AiChat, useAsStreamAdapter, type ChatItem } from '@nlux/react'
import type { StreamSend } from '@nlux/react'
import '@nlux/themes/nova.css'

interface CodingAssistantProps {
  open: boolean
  onClose: () => void
  datasetId: string
  getNotebookContent: () => string
  getToken?: () => Promise<string>
}

function createSend(
  datasetId: string,
  context: string,
  getToken?: () => Promise<string>
): StreamSend {
  return async (prompt, observer) => {
    try {
      const headers: Record<string, string> = { 'Content-Type': 'application/json' }
      if (getToken) {
        const token = await getToken()
        if (token) headers.Authorization = `Bearer ${token}`
      }

      const response = await fetch(
        `/code-suggestion/chat?datasetId=${encodeURIComponent(datasetId)}`,
        {
          method: 'POST',
          headers,
          body: JSON.stringify({ context, userInput: prompt }),
        }
      )

      if (response.status !== 200) {
        observer.error(new Error('Failed to connect to the server'))
        return
      }

      if (!response.body) {
        observer.complete()
        return
      }

      const reader = response.body.getReader()
      const decoder = new TextDecoder()

      try {
        while (true) {
          const { value, done } = await reader.read()
          if (done) break
          const chunk = decoder.decode(value, { stream: true })
          observer.next(chunk)
        }
      } finally {
        reader.releaseLock()
        observer.complete()
      }
    } catch (err) {
      observer.error(err instanceof Error ? err : new Error(String(err)))
    }
  }
}

export const CodingAssistant: FC<CodingAssistantProps> = ({
  open,
  onClose,
  datasetId,
  getNotebookContent,
  getToken,
}) => {
  const [conversationHistory] = useState<ChatItem[]>([])

  const content = getNotebookContent()
  const send = useMemo(
    () => createSend(datasetId, content, getToken),
    [datasetId, content, getToken]
  )
  const adapter = useAsStreamAdapter(send, [send])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation()
        onClose()
      }
    },
    [onClose]
  )

  if (!open) return null

  return (
    <div
      className="flex flex-col border-l border-border h-full overflow-hidden"
      style={{ flex: '4 1 0%', minWidth: 0 }}
      onKeyDown={handleKeyDown}
    >
      <div className="flex items-center justify-between border-b border-border bg-white px-3 py-2">
        <span className="text-sm font-medium text-[#000080]">Coding Assistant</span>
        <button
          className="bg-transparent border-0 outline-none cursor-pointer text-sm font-medium hover:opacity-70"
          style={{ color: '#000080' }}
          onClick={onClose}
        >
          Close
        </button>
      </div>
      <div className="flex-1 overflow-hidden" style={{ minHeight: 0 }}>
        <AiChat
          adapter={adapter}
          displayOptions={{ colorScheme: 'light' }}
          composerOptions={{ placeholder: 'Type your query' }}
          messageOptions={{ waitTimeBeforeStreamCompletion: 3000 }}
          initialConversation={conversationHistory}
        />
      </div>
      <style>{`
        .nlux_msg_sent,
        .nlux-comp-sendIcon-container {
          background-color: #000080 !important;
        }
        .nlux-comp-chatItem--received {
          padding-right: 30px !important;
        }
        .nlux-AiChat-root {
          height: 100% !important;
        }
        .nlux-chatSegments-container {
          flex: 1 1 0%;
          overflow: auto;
        }
      `}</style>
    </div>
  )
}

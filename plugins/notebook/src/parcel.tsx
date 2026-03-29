import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react'
import singleSpaReact from 'single-spa-react'
import ReactDOMClient from 'react-dom/client'
import { Notebook } from '@/components/notebook/Notebook'
import { PyodideKernel } from '@/kernels/pyodide/PyodideKernel'
import { WebRKernel } from '@/kernels/webr/WebRKernel'
import { parseIpynb, serializeIpynb } from '@/utils/serialization'
import type { NotebookData } from '@/types/notebook'
import type { KernelConfig } from '@/kernels/types'

// Don't import index.css — the host (DevX) already provides Tailwind theme variables.
// The notebook components use the same CSS variable names (--color-background, etc.)

interface NotebookParcelProps {
  content: string
  onContentChange?: (content: string) => void
  readOnly?: boolean
}

function NotebookParcelRoot({ content, onContentChange, readOnly }: NotebookParcelProps) {
  const [data, setData] = useState<NotebookData>(() => parseIpynb(content))
  const contentRef = useRef(content)

  useEffect(() => {
    if (content !== contentRef.current) {
      contentRef.current = content
      setData(parseIpynb(content))
    }
  }, [content])

  const handleChange = useCallback((d: NotebookData) => {
    setData(d)
    if (onContentChange) {
      const serialized = serializeIpynb(d)
      contentRef.current = serialized
      onContentChange(serialized)
    }
  }, [onContentChange])

  const kernels = useMemo(() => [new PyodideKernel(), new WebRKernel()], [])
  const kernelConfigs = useMemo<KernelConfig[]>(() => [
    { type: 'pyodide' },
    { type: 'webr' },
  ], [])

  return (
    <Notebook
      data={data}
      onChange={handleChange}
      showToolbar
      readOnly={readOnly ?? false}
      kernels={kernels}
      kernelConfigs={kernelConfigs}
    />
  )
}

const lifecycles = singleSpaReact({
  React,
  ReactDOMClient,
  rootComponent: NotebookParcelRoot,
  errorBoundary(_err: Error) {
    return <div className="p-4 text-red-500">Failed to load notebook</div>
  },
})

export const bootstrap = lifecycles.bootstrap
export const mount = lifecycles.mount
export const unmount = lifecycles.unmount

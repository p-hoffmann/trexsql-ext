import React, { useState, useRef, useCallback, useEffect } from 'react'
import {
  Notebook,
  NotebookHandle,
  NotebookData,
  PyodideKernel,
  createEmptyNotebook,
  parseIpynb,
  serializeIpynb,
} from '../../src'
import { Button } from '../../src/components/ui/button'
import { Download, Upload, FileText } from 'lucide-react'

const pyodideKernel = new PyodideKernel()

function App() {
  const [notebookData, setNotebookData] = useState<NotebookData>(createEmptyNotebook())
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false)
  const notebookRef = useRef<NotebookHandle>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      if (hasUnsavedChanges) {
        event.preventDefault()
        event.returnValue = '' // required by Chrome
        return ''
      }
    }

    window.addEventListener('beforeunload', handleBeforeUnload)
    return () => window.removeEventListener('beforeunload', handleBeforeUnload)
  }, [hasUnsavedChanges])

  const handleChange = useCallback((data: NotebookData) => {
    setNotebookData(data)
    setHasUnsavedChanges(true)
  }, [])

  const handleDownload = useCallback(() => {
    const content = serializeIpynb(notebookData)
    const blob = new Blob([content], { type: 'application/json' })
    const url = URL.createObjectURL(blob)

    const a = document.createElement('a')
    a.href = url
    a.download = `${notebookData.metadata.title || 'notebook'}.ipynb`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    setHasUnsavedChanges(false)
  }, [notebookData])

  const handleUploadClick = useCallback(() => {
    fileInputRef.current?.click()
  }, [])

  const handleFileChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    const reader = new FileReader()
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string
        const imported = parseIpynb(content)
        setNotebookData(imported)

        const titleFromFile = file.name.replace(/\.ipynb$/, '')
        setNotebookData((prev) => ({
          ...prev,
          metadata: {
            ...prev.metadata,
            title: titleFromFile,
          },
        }))
      } catch (error) {
        console.error('Failed to parse notebook:', error)
        alert('Failed to parse notebook file. Please check that it is a valid .ipynb file.')
      }
    }
    reader.readAsText(file)

    event.target.value = '' // allow re-selecting same file
  }, [])

  const handleNewNotebook = useCallback(() => {
    if (hasUnsavedChanges) {
      const confirmed = window.confirm(
        'Are you sure you want to create a new notebook? All unsaved changes will be lost.'
      )
      if (!confirmed) return
    }
    setNotebookData(createEmptyNotebook())
    setHasUnsavedChanges(false)
  }, [hasUnsavedChanges])

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="container flex h-14 items-center gap-4 px-4">
          <h1 className="text-lg font-semibold">React Notebook</h1>

          <div className="ml-auto flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={handleNewNotebook}>
              <FileText className="mr-2 h-4 w-4" />
              New
            </Button>
            <Button variant="outline" size="sm" onClick={handleUploadClick}>
              <Upload className="mr-2 h-4 w-4" />
              Open
            </Button>
            <Button variant="outline" size="sm" onClick={handleDownload}>
              <Download className="mr-2 h-4 w-4" />
              Save
            </Button>
          </div>
        </div>
      </header>

      <input
        ref={fileInputRef}
        type="file"
        accept=".ipynb"
        className="hidden"
        onChange={handleFileChange}
      />

      <main className="flex-1 container px-4 py-6">
        <Notebook
          ref={notebookRef}
          data={notebookData}
          onChange={handleChange}
          kernels={[pyodideKernel]}
          defaultKernelConfig={{ type: 'pyodide' }}
          showToolbar={true}
          showLineNumbers={true}
        />
      </main>

      <footer className="border-t py-4">
        <div className="container px-4 text-center text-sm text-muted-foreground">
          React Notebook Component Demo
        </div>
      </footer>
    </div>
  )
}

export default App

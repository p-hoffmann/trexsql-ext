import { useEffect, useRef, useState } from 'react'

interface RenameDialogProps {
  currentName: string
  onConfirm: (newName: string) => void
  onCancel: () => void
}

export function RenameDialog({ currentName, onConfirm, onCancel }: RenameDialogProps) {
  const [name, setName] = useState(currentName)
  const dialogRef = useRef<HTMLDialogElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    dialogRef.current?.showModal()
    inputRef.current?.select()
  }, [])

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const trimmed = name.trim()
    if (trimmed) {
      onConfirm(trimmed)
    }
  }

  return (
    <dialog
      ref={dialogRef}
      className="rounded-lg border bg-background p-6 shadow-lg backdrop:bg-black/50"
      onClose={onCancel}
    >
      <h2 className="text-lg font-semibold">Rename Notebook</h2>
      <form onSubmit={handleSubmit}>
        <input
          ref={inputRef}
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="mt-3 w-full rounded border border-input bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
          placeholder="Notebook name"
        />
        <div className="mt-4 flex justify-end gap-2">
          <button
            type="button"
            className="rounded border border-input px-3 py-1.5 text-sm hover:bg-accent"
            onClick={onCancel}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
            disabled={!name.trim()}
          >
            Rename
          </button>
        </div>
      </form>
    </dialog>
  )
}

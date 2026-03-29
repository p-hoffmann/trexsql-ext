import { useEffect, useRef } from 'react'

interface DeleteDialogProps {
  notebookName: string
  onConfirm: () => void
  onCancel: () => void
}

export function DeleteDialog({ notebookName, onConfirm, onCancel }: DeleteDialogProps) {
  const dialogRef = useRef<HTMLDialogElement>(null)

  useEffect(() => {
    dialogRef.current?.showModal()
  }, [])

  return (
    <dialog
      ref={dialogRef}
      className="rounded-lg border bg-background p-6 shadow-lg backdrop:bg-black/50"
      onClose={onCancel}
    >
      <h2 className="text-lg font-semibold">Delete Notebook</h2>
      <p className="mt-2 text-sm text-muted-foreground">
        Are you sure you want to delete <strong>{notebookName}</strong>? This action cannot be
        undone.
      </p>
      <div className="mt-4 flex justify-end gap-2">
        <button
          className="rounded border border-input px-3 py-1.5 text-sm hover:bg-accent"
          onClick={onCancel}
        >
          Cancel
        </button>
        <button
          className="rounded bg-destructive px-3 py-1.5 text-sm text-destructive-foreground hover:bg-destructive/90"
          onClick={onConfirm}
        >
          Delete
        </button>
      </div>
    </dialog>
  )
}

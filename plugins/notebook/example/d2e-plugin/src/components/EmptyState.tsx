interface EmptyStateProps {
  hasNotebooks: boolean
  onCreate: () => void
  onImport: () => void
}

export function EmptyState({ hasNotebooks, onCreate, onImport }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center gap-4 rounded-lg border-2 border-dashed border-muted py-16">
      <p className="text-muted-foreground">
        {hasNotebooks ? 'Select a notebook to get started' : 'No notebooks yet'}
      </p>
      <div className="flex gap-2">
        <button
          className="rounded bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
          onClick={onCreate}
        >
          Create Notebook
        </button>
        <button
          className="rounded border border-input px-4 py-2 text-sm hover:bg-accent"
          onClick={onImport}
        >
          Import .ipynb
        </button>
      </div>
    </div>
  )
}

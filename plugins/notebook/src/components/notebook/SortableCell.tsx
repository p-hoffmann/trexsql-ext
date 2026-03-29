import React from 'react'
import { useSortable } from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { GripVertical } from 'lucide-react'
import { Cell, type CellProps } from './Cell'
import { cn } from '@/lib/utils'

export interface SortableCellProps extends CellProps {
  id: string
  /** Enable content-visibility for performance with large notebooks */
  useVirtualization?: boolean
}

export function SortableCell({ id, useVirtualization, ...cellProps }: SortableCellProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id })

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    ...(useVirtualization && {
      contentVisibility: 'auto' as const,
      containIntrinsicSize: 'auto 150px', // Estimated cell height
    }),
  }

  return (
    <div ref={setNodeRef} style={style} className="relative group">
      <div
        {...attributes}
        {...listeners}
        className={cn(
          'absolute left-0 top-1/2 -translate-y-1/2 z-30 p-1 cursor-grab active:cursor-grabbing',
          'opacity-0 group-hover:opacity-100 transition-opacity',
          'hover:bg-accent rounded',
          cellProps.isSelected && 'opacity-100'
        )}
        title="Drag to reorder"
      >
        <GripVertical className="h-4 w-4 text-muted-foreground" />
      </div>
      <Cell {...cellProps} />
    </div>
  )
}

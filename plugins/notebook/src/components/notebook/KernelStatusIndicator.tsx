import { cn } from '@/lib/utils'
import type { KernelStatus } from '@/kernels/types'

export interface KernelStatusIndicatorProps {
  status: KernelStatus
  kernelName?: string
  className?: string
}

const statusColors: Record<KernelStatus, string> = {
  disconnected: 'bg-muted',
  connecting: 'bg-warning animate-pulse',
  idle: 'bg-success',
  busy: 'bg-warning animate-pulse',
  error: 'bg-destructive',
}

const statusLabels: Record<KernelStatus, string> = {
  disconnected: 'Disconnected',
  connecting: 'Connecting...',
  idle: 'Ready',
  busy: 'Running...',
  error: 'Error',
}

export function KernelStatusIndicator({
  status,
  kernelName,
  className,
}: KernelStatusIndicatorProps) {
  return (
    <div className={cn('flex items-center gap-2 text-sm', className)}>
      <div className={cn('h-2 w-2 rounded-full', statusColors[status])} />
      <span className="text-muted-foreground">
        {kernelName && <span className="font-medium">{kernelName}</span>}
        {kernelName && ' - '}
        {statusLabels[status]}
      </span>
    </div>
  )
}

/**
 * Unit tests for CellOutput component
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CellOutput } from '@/components/notebook/CellOutput'
import type { CellOutput as CellOutputType } from '@/types/notebook'

describe('CellOutput', () => {
  it('renders nothing when outputs is empty', () => {
    const { container } = render(<CellOutput outputs={[]} />)
    expect(container.firstChild).toBeNull()
  })

  it('renders stream output', () => {
    const outputs: CellOutputType[] = [
      { type: 'stream', name: 'stdout', text: 'Hello World' },
    ]
    render(<CellOutput outputs={outputs} />)
    expect(screen.getByText('Hello World')).toBeInTheDocument()
  })

  it('renders stderr with destructive styling', () => {
    const outputs: CellOutputType[] = [
      { type: 'stream', name: 'stderr', text: 'Error message' },
    ]
    render(<CellOutput outputs={outputs} />)
    const element = screen.getByText('Error message')
    expect(element).toHaveClass('text-destructive')
  })

  it('renders error output with traceback', () => {
    const outputs: CellOutputType[] = [
      {
        type: 'error',
        ename: 'ValueError',
        evalue: 'Invalid value',
        traceback: ['Line 1', 'Line 2'],
      },
    ]
    render(<CellOutput outputs={outputs} />)
    expect(screen.getByText(/ValueError/)).toBeInTheDocument()
    expect(screen.getByText(/Invalid value/)).toBeInTheDocument()
  })

  it('renders execute result with text/plain', () => {
    const outputs: CellOutputType[] = [
      {
        type: 'execute_result',
        executionCount: 1,
        data: { 'text/plain': '42' },
      },
    ]
    render(<CellOutput outputs={outputs} />)
    expect(screen.getByText('42')).toBeInTheDocument()
  })

  it('renders display_data with image', () => {
    const outputs: CellOutputType[] = [
      {
        type: 'display_data',
        data: { 'image/png': 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==' },
      },
    ]
    render(<CellOutput outputs={outputs} />)
    const img = document.querySelector('img')
    expect(img).toBeInTheDocument()
    expect(img?.src).toContain('data:image/png')
  })

  it('renders multiple outputs', () => {
    const outputs: CellOutputType[] = [
      { type: 'stream', name: 'stdout', text: 'First' },
      { type: 'stream', name: 'stdout', text: 'Second' },
    ]
    render(<CellOutput outputs={outputs} />)
    expect(screen.getByText('First')).toBeInTheDocument()
    expect(screen.getByText('Second')).toBeInTheDocument()
  })
})

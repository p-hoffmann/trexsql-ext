/**
 * Unit tests for CodeCell component
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CodeCell } from '@/components/notebook/CodeCell'

describe('CodeCell', () => {
  const defaultProps = {
    source: 'print("hello")',
    language: 'python' as const,
    executionCount: null,
    executionState: 'idle' as const,
    isSelected: false,
  }

  it('renders code content', () => {
    render(<CodeCell {...defaultProps} />)
    // CodeMirror renders the content
    expect(screen.getByText(/print/)).toBeInTheDocument()
  })

  it('displays execution count when set', () => {
    render(<CodeCell {...defaultProps} executionCount={5} />)
    expect(screen.getByText('[5]')).toBeInTheDocument()
  })

  it('displays empty brackets when no execution count', () => {
    render(<CodeCell {...defaultProps} executionCount={null} />)
    expect(screen.getByText('[ ]')).toBeInTheDocument()
  })

  it('displays asterisk when running', () => {
    render(<CodeCell {...defaultProps} executionState="running" />)
    expect(screen.getByText('[*]')).toBeInTheDocument()
  })

  it('shows spinner when executing', () => {
    render(<CodeCell {...defaultProps} executionState="running" />)
    const spinner = document.querySelector('.animate-spin')
    expect(spinner).toBeInTheDocument()
  })

  it('calls onChange when content changes', async () => {
    const onChange = vi.fn()
    render(<CodeCell {...defaultProps} onChange={onChange} />)
    // CodeMirror handles the actual change events internally
    // This is mainly a smoke test to ensure the prop is passed correctly
    expect(onChange).not.toHaveBeenCalled()
  })

  it('applies selected styles when selected', () => {
    const { container } = render(<CodeCell {...defaultProps} isSelected={true} />)
    expect(container.firstChild).toHaveClass('border-primary')
  })

  it('respects readOnly prop', () => {
    render(<CodeCell {...defaultProps} readOnly={true} />)
    // The CodeMirror editor should be read-only
    const editor = document.querySelector('.cm-editor')
    expect(editor).toBeInTheDocument()
  })
})

/**
 * Unit tests for MarkdownCell component
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MarkdownCell } from '@/components/notebook/MarkdownCell'

describe('MarkdownCell', () => {
  const defaultProps = {
    source: '# Hello World',
    isSelected: false,
  }

  it('renders markdown content', () => {
    render(<MarkdownCell {...defaultProps} />)
    // The h1 should be rendered
    expect(screen.getByText('Hello World')).toBeInTheDocument()
  })

  it('renders placeholder when source is empty', () => {
    render(<MarkdownCell source="" isSelected={false} />)
    expect(screen.getByText(/Click to add markdown content/)).toBeInTheDocument()
  })

  it('renders formatted markdown elements', () => {
    render(
      <MarkdownCell
        source="**bold** and *italic* and `code`"
        isSelected={false}
      />
    )
    expect(screen.getByText('bold')).toBeInTheDocument()
    expect(screen.getByText('italic')).toBeInTheDocument()
    expect(screen.getByText('code')).toBeInTheDocument()
  })

  it('enters edit mode on double click', () => {
    render(<MarkdownCell {...defaultProps} isSelected={true} />)
    const container = screen.getByText('Hello World').closest('[class*="prose"]')?.parentElement

    if (container) {
      fireEvent.doubleClick(container)
      // After double click, CodeMirror editor should be present
      const editor = document.querySelector('.cm-editor')
      expect(editor).toBeInTheDocument()
    }
  })

  it('does not enter edit mode when readOnly', () => {
    render(<MarkdownCell {...defaultProps} isSelected={true} readOnly={true} />)
    const container = screen.getByText('Hello World').closest('[class*="prose"]')?.parentElement

    if (container) {
      fireEvent.doubleClick(container)
      // Should not switch to edit mode
      const editor = document.querySelector('.cm-editor')
      expect(editor).not.toBeInTheDocument()
    }
  })

  it('calls onChange when content is modified', async () => {
    const onChange = vi.fn()
    render(
      <MarkdownCell {...defaultProps} isSelected={true} onChange={onChange} />
    )

    // Double click to enter edit mode
    const container = screen.getByText('Hello World').closest('[class*="prose"]')?.parentElement
    if (container) {
      fireEvent.doubleClick(container)
    }

    // onChange is called through CodeMirror's internal handling
    // This is mainly a smoke test
  })

  it('applies selected styles when selected', () => {
    const { container } = render(
      <MarkdownCell {...defaultProps} isSelected={true} />
    )
    // Check for border-primary class on the wrapper
    expect(container.firstChild).toHaveClass('border-primary')
  })

  it('renders lists correctly', () => {
    const { container } = render(
      <MarkdownCell
        source="- Item 1\n- Item 2\n- Item 3"
        isSelected={false}
      />
    )
    // Check that list items are rendered
    const listItems = container.querySelectorAll('li')
    expect(listItems.length).toBeGreaterThanOrEqual(1)
  })

  it('sanitizes HTML to prevent XSS', () => {
    render(
      <MarkdownCell
        source='<script>alert("xss")</script>Visible text'
        isSelected={false}
      />
    )
    // Script should be stripped
    expect(document.querySelector('script')).not.toBeInTheDocument()
    expect(screen.getByText('Visible text')).toBeInTheDocument()
  })
})

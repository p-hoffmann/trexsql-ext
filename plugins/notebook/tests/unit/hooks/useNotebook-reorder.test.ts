/**
 * Unit tests for cell reordering in useNotebook hook
 */

import { describe, it, expect } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useNotebook } from '@/hooks/useNotebook'

describe('useNotebook - cell reordering', () => {
  describe('moveCell', () => {
    it('moves cell from first to second position', () => {
      const { result } = renderHook(() => useNotebook())

      // Add 3 cells
      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const cellIds = result.current.notebook.cells.map((c) => c.id)
      const [first, second, third] = cellIds

      // Move first cell to position 1 (second position)
      act(() => {
        result.current.actions.moveCell(first, 1)
      })

      const newIds = result.current.notebook.cells.map((c) => c.id)
      expect(newIds).toEqual([second, first, third])
    })

    it('moves cell from last to first position', () => {
      const { result } = renderHook(() => useNotebook())

      // Add 3 cells
      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const cellIds = result.current.notebook.cells.map((c) => c.id)
      const [first, second, third] = cellIds

      // Move last cell to position 0 (first position)
      act(() => {
        result.current.actions.moveCell(third, 0)
      })

      const newIds = result.current.notebook.cells.map((c) => c.id)
      expect(newIds).toEqual([third, first, second])
    })

    it('moves cell from middle to end', () => {
      const { result } = renderHook(() => useNotebook())

      // Add 3 cells
      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const cellIds = result.current.notebook.cells.map((c) => c.id)
      const [first, second, third] = cellIds

      // Move second cell to position 2 (last position)
      act(() => {
        result.current.actions.moveCell(second, 2)
      })

      const newIds = result.current.notebook.cells.map((c) => c.id)
      expect(newIds).toEqual([first, third, second])
    })

    it('does nothing when moving to same position', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const cellIds = result.current.notebook.cells.map((c) => c.id)

      // Move first cell to position 0 (same position)
      act(() => {
        result.current.actions.moveCell(cellIds[0], 0)
      })

      const newIds = result.current.notebook.cells.map((c) => c.id)
      expect(newIds).toEqual(cellIds)
    })

    it('handles invalid cell id gracefully', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
      })

      const originalCells = [...result.current.notebook.cells]

      // Try to move non-existent cell
      act(() => {
        result.current.actions.moveCell('non-existent-id', 0)
      })

      // Cells should be unchanged
      expect(result.current.notebook.cells).toEqual(originalCells)
    })

    it('clamps position to valid range', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const cellIds = result.current.notebook.cells.map((c) => c.id)
      const [first, second] = cellIds

      // Move to position beyond array length
      act(() => {
        result.current.actions.moveCell(first, 100)
      })

      const newIds = result.current.notebook.cells.map((c) => c.id)
      // First should now be at the end
      expect(newIds).toEqual([second, first])
    })

    it('clamps negative position to 0', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const cellIds = result.current.notebook.cells.map((c) => c.id)
      const [first, second] = cellIds

      // Move to negative position
      act(() => {
        result.current.actions.moveCell(second, -5)
      })

      const newIds = result.current.notebook.cells.map((c) => c.id)
      // Second should now be at the beginning
      expect(newIds).toEqual([second, first])
    })

    it('preserves cell content after move', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('markdown')
      })

      const [codeCell, markdownCell] = result.current.notebook.cells

      // Update source content
      act(() => {
        result.current.actions.updateCellSource(codeCell.id, 'print("hello")')
        result.current.actions.updateCellSource(markdownCell.id, '# Title')
      })

      // Move cells
      act(() => {
        result.current.actions.moveCell(markdownCell.id, 0)
      })

      // Verify content is preserved
      const [first, second] = result.current.notebook.cells
      expect(first.id).toBe(markdownCell.id)
      expect(first.source).toBe('# Title')
      expect(second.id).toBe(codeCell.id)
      expect(second.source).toBe('print("hello")')
    })

    it('is tracked in undo history', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
      })

      const originalOrder = result.current.notebook.cells.map((c) => c.id)
      const [first] = originalOrder

      // Move cell
      act(() => {
        result.current.actions.moveCell(first, 1)
      })

      expect(result.current.history.canUndo).toBe(true)

      // Undo should restore original order
      act(() => {
        result.current.actions.undo()
      })

      const restoredOrder = result.current.notebook.cells.map((c) => c.id)
      expect(restoredOrder).toEqual(originalOrder)
    })
  })

  describe('multiple cell operations', () => {
    it('handles sequence of add, move, delete operations', () => {
      const { result } = renderHook(() => useNotebook())

      // Add cells
      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('code', undefined, 'python')
        result.current.actions.addCell('markdown')
      })

      const [first, second, third] = result.current.notebook.cells.map((c) => c.id)

      // Move second to first
      act(() => {
        result.current.actions.moveCell(second, 0)
      })

      // Delete first (which is now second)
      act(() => {
        result.current.actions.deleteCell(second)
      })

      // Verify remaining cells
      const remaining = result.current.notebook.cells.map((c) => c.id)
      expect(remaining).toEqual([first, third])
    })
  })
})

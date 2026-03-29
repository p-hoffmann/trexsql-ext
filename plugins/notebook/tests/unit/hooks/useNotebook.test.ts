/**
 * Unit tests for useNotebook hook
 */

import { describe, it, expect, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useNotebook } from '@/hooks/useNotebook'
import type { NotebookData } from '@/types/notebook'

describe('useNotebook', () => {
  const initialData: NotebookData = {
    metadata: { title: 'Test Notebook' },
    cells: [
      {
        id: 'cell-1',
        type: 'code',
        language: 'python',
        source: 'print("hello")',
        executionCount: null,
        executionState: 'idle',
        outputs: [],
      },
    ],
  }

  describe('addCell', () => {
    it('adds a code cell with specified language', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code', undefined, 'python')
      })

      expect(result.current.notebook.cells).toHaveLength(1)
      expect(result.current.notebook.cells[0].type).toBe('code')
      if (result.current.notebook.cells[0].type === 'code') {
        expect(result.current.notebook.cells[0].language).toBe('python')
      }
    })

    it('adds a markdown cell', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('markdown')
      })

      expect(result.current.notebook.cells).toHaveLength(1)
      expect(result.current.notebook.cells[0].type).toBe('markdown')
    })

    it('inserts cell at specified position', () => {
      const { result } = renderHook(() => useNotebook({ initialData }))

      act(() => {
        result.current.actions.addCell('code', 0, 'r')
      })

      expect(result.current.notebook.cells).toHaveLength(2)
      if (result.current.notebook.cells[0].type === 'code') {
        expect(result.current.notebook.cells[0].language).toBe('r')
      }
    })

    it('selects newly added cell', () => {
      const { result } = renderHook(() => useNotebook())

      let newCellId: string
      act(() => {
        newCellId = result.current.actions.addCell('code')
      })

      expect(result.current.selectedCellId).toBe(newCellId!)
    })
  })

  describe('updateCellSource', () => {
    it('updates cell source content', () => {
      const { result } = renderHook(() => useNotebook({ initialData }))

      act(() => {
        result.current.actions.updateCellSource('cell-1', 'new content')
      })

      expect(result.current.notebook.cells[0].source).toBe('new content')
    })

    it('calls onChange callback', () => {
      const onChange = vi.fn()
      const { result } = renderHook(() =>
        useNotebook({ initialData, onChange })
      )

      act(() => {
        result.current.actions.updateCellSource('cell-1', 'new content')
      })

      expect(onChange).toHaveBeenCalled()
    })
  })

  describe('deleteCell', () => {
    it('removes the cell from notebook', () => {
      const { result } = renderHook(() => useNotebook({ initialData }))

      act(() => {
        result.current.actions.deleteCell('cell-1')
      })

      expect(result.current.notebook.cells).toHaveLength(0)
    })

    it('updates selection when deleting selected cell', () => {
      const multiCellData: NotebookData = {
        metadata: {},
        cells: [
          { id: 'a', type: 'code', language: 'python', source: '', executionCount: null, executionState: 'idle', outputs: [] },
          { id: 'b', type: 'code', language: 'python', source: '', executionCount: null, executionState: 'idle', outputs: [] },
        ],
      }
      const { result } = renderHook(() => useNotebook({ initialData: multiCellData }))

      act(() => {
        result.current.actions.selectCell('a')
      })

      act(() => {
        result.current.actions.deleteCell('a')
      })

      expect(result.current.selectedCellId).toBe('b')
    })
  })

  describe('moveCell', () => {
    it('moves cell to new position', () => {
      const multiCellData: NotebookData = {
        metadata: {},
        cells: [
          { id: 'a', type: 'code', language: 'python', source: 'first', executionCount: null, executionState: 'idle', outputs: [] },
          { id: 'b', type: 'code', language: 'python', source: 'second', executionCount: null, executionState: 'idle', outputs: [] },
          { id: 'c', type: 'code', language: 'python', source: 'third', executionCount: null, executionState: 'idle', outputs: [] },
        ],
      }
      const { result } = renderHook(() => useNotebook({ initialData: multiCellData }))

      act(() => {
        result.current.actions.moveCell('c', 0)
      })

      expect(result.current.notebook.cells[0].id).toBe('c')
      expect(result.current.notebook.cells[1].id).toBe('a')
      expect(result.current.notebook.cells[2].id).toBe('b')
    })
  })

  describe('undo/redo', () => {
    it('can undo cell addition', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code')
      })

      expect(result.current.notebook.cells).toHaveLength(1)
      expect(result.current.history.canUndo).toBe(true)

      act(() => {
        result.current.actions.undo()
      })

      expect(result.current.notebook.cells).toHaveLength(0)
    })

    it('can redo after undo', () => {
      const { result } = renderHook(() => useNotebook())

      act(() => {
        result.current.actions.addCell('code')
      })

      act(() => {
        result.current.actions.undo()
      })

      expect(result.current.history.canRedo).toBe(true)

      act(() => {
        result.current.actions.redo()
      })

      expect(result.current.notebook.cells).toHaveLength(1)
    })
  })

  describe('selectCell', () => {
    it('updates selectedCellId', () => {
      const { result } = renderHook(() => useNotebook({ initialData }))

      act(() => {
        result.current.actions.selectCell('cell-1')
      })

      expect(result.current.selectedCellId).toBe('cell-1')
    })

    it('can clear selection', () => {
      const { result } = renderHook(() => useNotebook({ initialData }))

      act(() => {
        result.current.actions.selectCell('cell-1')
      })

      act(() => {
        result.current.actions.selectCell(null)
      })

      expect(result.current.selectedCellId).toBeNull()
    })
  })
})

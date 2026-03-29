/**
 * Unit tests for serialization utilities
 */

import { describe, it, expect } from 'vitest'
import {
  toIpynb,
  fromIpynb,
  parseIpynb,
  serializeIpynb,
} from '@/utils/serialization'
import { createCodeCell, createMarkdownCell, createEmptyNotebook } from '@/types/notebook'
import type { NotebookData } from '@/types/notebook'
import type { IpynbNotebook } from '@/types/ipynb'

describe('serialization', () => {
  describe('toIpynb', () => {
    it('converts empty notebook', () => {
      const notebook = createEmptyNotebook()
      const ipynb = toIpynb(notebook)

      expect(ipynb.nbformat).toBe(4)
      expect(ipynb.nbformat_minor).toBeGreaterThanOrEqual(5)
      expect(ipynb.cells).toEqual([])
      expect(ipynb.metadata.kernelspec).toBeDefined()
    })

    it('converts code cells', () => {
      const notebook: NotebookData = {
        ...createEmptyNotebook(),
        cells: [
          {
            ...createCodeCell('python'),
            source: 'print("hello")',
            executionCount: 1,
            outputs: [
              {
                type: 'stream',
                name: 'stdout',
                text: 'hello\n',
              },
            ],
          },
        ],
      }

      const ipynb = toIpynb(notebook)

      expect(ipynb.cells.length).toBe(1)
      expect(ipynb.cells[0].cell_type).toBe('code')
      expect(ipynb.cells[0].source).toBe('print("hello")')

      const codeCell = ipynb.cells[0] as { outputs: unknown[] }
      expect(codeCell.outputs.length).toBe(1)
    })

    it('converts markdown cells', () => {
      const notebook: NotebookData = {
        ...createEmptyNotebook(),
        cells: [
          {
            ...createMarkdownCell(),
            source: '# Hello World',
          },
        ],
      }

      const ipynb = toIpynb(notebook)

      expect(ipynb.cells.length).toBe(1)
      expect(ipynb.cells[0].cell_type).toBe('markdown')
      expect(ipynb.cells[0].source).toBe('# Hello World')
    })

    it('sets kernelspec based on first code cell language', () => {
      const notebook: NotebookData = {
        ...createEmptyNotebook(),
        cells: [
          {
            ...createCodeCell('r'),
            source: 'print("hello")',
          },
        ],
      }

      const ipynb = toIpynb(notebook)

      expect(ipynb.metadata.kernelspec?.name).toBe('ir')
      expect(ipynb.metadata.kernelspec?.language).toBe('R')
    })
  })

  describe('fromIpynb', () => {
    it('converts empty notebook', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [],
      }

      const notebook = fromIpynb(ipynb)

      expect(notebook.cells).toEqual([])
      expect(notebook.metadata).toBeDefined()
    })

    it('converts code cells', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'code',
            source: 'print("hello")',
            execution_count: 1,
            outputs: [
              {
                output_type: 'stream',
                name: 'stdout',
                text: 'hello\n',
              },
            ],
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)

      expect(notebook.cells.length).toBe(1)
      expect(notebook.cells[0].type).toBe('code')
      expect(notebook.cells[0].source).toBe('print("hello")')
    })

    it('normalizes source arrays to strings', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'code',
            source: ['line1\n', 'line2\n', 'line3'],
            execution_count: null,
            outputs: [],
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)

      expect(notebook.cells[0].source).toBe('line1\nline2\nline3')
    })

    it('generates cell IDs if missing', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'markdown',
            source: '# Hello',
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)

      expect(notebook.cells[0].id).toBeDefined()
      expect(notebook.cells[0].id.length).toBeGreaterThan(0)
    })

    it('detects R language from kernelspec', () => {
      const ipynb: IpynbNotebook = {
        metadata: {
          kernelspec: {
            name: 'ir',
            language: 'R',
            display_name: 'R',
          },
        },
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'code',
            source: 'print("hello")',
            execution_count: null,
            outputs: [],
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)

      expect(notebook.cells[0].type).toBe('code')
      if (notebook.cells[0].type === 'code') {
        expect(notebook.cells[0].language).toBe('r')
      }
    })
  })

  describe('parseIpynb / serializeIpynb', () => {
    it('round-trips notebook data', () => {
      const original: NotebookData = {
        ...createEmptyNotebook(),
        cells: [
          {
            ...createCodeCell('python'),
            source: 'x = 1 + 2',
            executionCount: 1,
            outputs: [
              {
                type: 'execute_result',
                executionCount: 1,
                data: { 'text/plain': '3' },
              },
            ],
          },
          {
            ...createMarkdownCell(),
            source: '# Results',
          },
        ],
      }

      const json = serializeIpynb(original)
      const restored = parseIpynb(json)

      expect(restored.cells.length).toBe(2)
      expect(restored.cells[0].source).toBe('x = 1 + 2')
      expect(restored.cells[1].source).toBe('# Results')
    })

    it('produces valid JSON', () => {
      const notebook = createEmptyNotebook()
      const json = serializeIpynb(notebook)

      expect(() => JSON.parse(json)).not.toThrow()
    })
  })

  describe('output conversion', () => {
    it('handles stream output', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'code',
            source: '',
            execution_count: 1,
            outputs: [
              {
                output_type: 'stream',
                name: 'stderr',
                text: 'Warning message',
              },
            ],
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)
      const cell = notebook.cells[0]

      if (cell.type === 'code' && cell.outputs[0].type === 'stream') {
        expect(cell.outputs[0].name).toBe('stderr')
        expect(cell.outputs[0].text).toBe('Warning message')
      }
    })

    it('handles display_data output', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'code',
            source: '',
            execution_count: 1,
            outputs: [
              {
                output_type: 'display_data',
                data: {
                  'image/png': 'base64data',
                  'text/plain': '<Figure>',
                },
                metadata: {},
              },
            ],
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)
      const cell = notebook.cells[0]

      if (cell.type === 'code' && cell.outputs[0].type === 'display_data') {
        expect(cell.outputs[0].data['image/png']).toBe('base64data')
        expect(cell.outputs[0].data['text/plain']).toBe('<Figure>')
      }
    })

    it('handles error output', () => {
      const ipynb: IpynbNotebook = {
        metadata: {},
        nbformat: 4,
        nbformat_minor: 5,
        cells: [
          {
            cell_type: 'code',
            source: '',
            execution_count: 1,
            outputs: [
              {
                output_type: 'error',
                ename: 'ValueError',
                evalue: 'invalid value',
                traceback: ['Traceback...', '  File...', 'ValueError: invalid value'],
              },
            ],
            metadata: {},
          },
        ],
      }

      const notebook = fromIpynb(ipynb)
      const cell = notebook.cells[0]

      if (cell.type === 'code' && cell.outputs[0].type === 'error') {
        expect(cell.outputs[0].ename).toBe('ValueError')
        expect(cell.outputs[0].evalue).toBe('invalid value')
        expect(cell.outputs[0].traceback.length).toBe(3)
      }
    })
  })
})

import type { NotebookData, CellData, CellOutput, CellLanguage } from '@/types/notebook'
import { createCodeCell, createMarkdownCell, isCodeCell } from '@/types/notebook'
import type {
  IpynbNotebook,
  IpynbCell,
  IpynbCodeCell,
  IpynbMarkdownCell,
  IpynbOutput,
  IpynbStreamOutput,
  IpynbDisplayData,
  IpynbExecuteResult,
  IpynbError,
} from '@/types/ipynb'

// ipynb source/text fields can be string or string[]
function normalizeSource(source: string | string[]): string {
  if (Array.isArray(source)) {
    return source.join('')
  }
  return source
}

function normalizeText(text: string | string[]): string {
  if (Array.isArray(text)) {
    return text.join('')
  }
  return text
}

function convertIpynbOutput(output: IpynbOutput): CellOutput {
  switch (output.output_type) {
    case 'stream':
      return {
        type: 'stream',
        name: output.name,
        text: normalizeText(output.text),
      }

    case 'display_data': {
      const data: Record<string, string> = {}
      for (const [mimeType, content] of Object.entries(output.data)) {
        if (content !== undefined) {
          data[mimeType] = typeof content === 'string' ? content :
            Array.isArray(content) ? content.join('') : JSON.stringify(content)
        }
      }
      return {
        type: 'display_data',
        data,
        metadata: output.metadata,
      }
    }

    case 'execute_result': {
      const data: Record<string, string> = {}
      for (const [mimeType, content] of Object.entries(output.data)) {
        if (content !== undefined) {
          data[mimeType] = typeof content === 'string' ? content :
            Array.isArray(content) ? content.join('') : JSON.stringify(content)
        }
      }
      return {
        type: 'execute_result',
        executionCount: output.execution_count,
        data,
        metadata: output.metadata,
      }
    }

    case 'error':
      return {
        type: 'error',
        ename: output.ename,
        evalue: output.evalue,
        traceback: output.traceback,
      }

    default:
      return {
        type: 'display_data',
        data: { 'text/plain': JSON.stringify(output) },
        metadata: {},
      }
  }
}

function convertToIpynbOutput(output: CellOutput): IpynbOutput {
  switch (output.type) {
    case 'stream':
      return {
        output_type: 'stream',
        name: output.name,
        text: output.text,
      } as IpynbStreamOutput

    case 'display_data':
      return {
        output_type: 'display_data',
        data: output.data,
        metadata: output.metadata ?? {},
      } as IpynbDisplayData

    case 'execute_result':
      return {
        output_type: 'execute_result',
        execution_count: output.executionCount,
        data: output.data,
        metadata: output.metadata ?? {},
      } as IpynbExecuteResult

    case 'error':
      return {
        output_type: 'error',
        ename: output.ename,
        evalue: output.evalue,
        traceback: output.traceback,
      } as IpynbError
  }
}

function detectLanguage(ipynb: IpynbNotebook): CellLanguage {
  const langInfo = ipynb.metadata.language_info?.name?.toLowerCase()
  const kernelLang = ipynb.metadata.kernelspec?.language?.toLowerCase()
  const kernelName = ipynb.metadata.kernelspec?.name?.toLowerCase()

  if (langInfo === 'r' || kernelLang === 'r' || kernelName?.includes('ir')) {
    return 'r'
  }

  return 'python'
}

function convertIpynbCell(cell: IpynbCell, defaultLanguage: CellLanguage): CellData {
  const source = normalizeSource(cell.source)

  if (cell.cell_type === 'code') {
    const codeCell = cell as IpynbCodeCell
    const newCell = createCodeCell(defaultLanguage)
    return {
      ...newCell,
      id: cell.id ?? newCell.id,
      source,
      executionCount: codeCell.execution_count,
      outputs: codeCell.outputs.map(convertIpynbOutput),
      executionState: 'idle',
    }
  }

  if (cell.cell_type === 'markdown') {
    const newCell = createMarkdownCell()
    return {
      ...newCell,
      id: cell.id ?? newCell.id,
      source,
    }
  }

  const newCell = createMarkdownCell()
  return {
    ...newCell,
    id: cell.id ?? newCell.id,
    source,
  }
}

function convertToIpynbCell(cell: CellData): IpynbCell {
  if (isCodeCell(cell)) {
    return {
      cell_type: 'code',
      id: cell.id,
      metadata: {},
      source: cell.source,
      execution_count: cell.executionCount,
      outputs: cell.outputs.map(convertToIpynbOutput),
    } as IpynbCodeCell
  }

  return {
    cell_type: 'markdown',
    id: cell.id,
    metadata: {},
    source: cell.source,
  } as IpynbMarkdownCell
}

export function fromIpynb(ipynb: IpynbNotebook): NotebookData {
  const defaultLanguage = detectLanguage(ipynb)

  return {
    metadata: {
      title: (ipynb.metadata.title as string) ?? 'Untitled',
      createdAt: (ipynb.metadata.created_at as string) ?? new Date().toISOString(),
      modifiedAt: new Date().toISOString(),
      kernelId: ipynb.metadata.kernelspec?.name,
    },
    cells: ipynb.cells.map((cell) => convertIpynbCell(cell, defaultLanguage)),
  }
}

export function toIpynb(notebook: NotebookData): IpynbNotebook {
  const codeCells = notebook.cells.filter(isCodeCell)
  const primaryLanguage = codeCells.length > 0 ? codeCells[0].language : 'python'

  const kernelspecs: Record<CellLanguage, { display_name: string; language: string; name: string }> = {
    python: {
      display_name: 'Python 3',
      language: 'python',
      name: 'python3',
    },
    r: {
      display_name: 'R',
      language: 'R',
      name: 'ir',
    },
  }

  return {
    metadata: {
      title: notebook.metadata.title,
      created_at: notebook.metadata.createdAt,
      kernelspec: kernelspecs[primaryLanguage],
      language_info: {
        name: primaryLanguage,
      },
    },
    nbformat: 4,
    nbformat_minor: 5,
    cells: notebook.cells.map(convertToIpynbCell),
  }
}

export function parseIpynb(content: string): NotebookData {
  const ipynb = JSON.parse(content) as IpynbNotebook
  return fromIpynb(ipynb)
}

export function serializeIpynb(notebook: NotebookData): string {
  const ipynb = toIpynb(notebook)
  return JSON.stringify(ipynb, null, 2)
}

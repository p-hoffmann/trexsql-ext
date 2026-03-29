export type CellId = string

export type CellLanguage = 'python' | 'r'

export type CellType = 'code' | 'markdown'

export type ExecutionState = 'idle' | 'queued' | 'running' | 'success' | 'error'

export interface MimeBundle {
  'text/plain'?: string
  'text/html'?: string
  'text/markdown'?: string
  'image/png'?: string
  'image/svg+xml'?: string
  'image/jpeg'?: string
  'application/json'?: unknown
  [mimeType: string]: unknown
}

export interface StreamCellOutput {
  type: 'stream'
  name: 'stdout' | 'stderr'
  text: string
}

export interface DisplayDataCellOutput {
  type: 'display_data'
  data: MimeBundle
  metadata?: Record<string, unknown>
}

export interface ExecuteResultCellOutput {
  type: 'execute_result'
  executionCount: number
  data: MimeBundle
  metadata?: Record<string, unknown>
}

export interface ErrorCellOutput {
  type: 'error'
  ename: string
  evalue: string
  traceback: string[]
}

export type CellOutput =
  | StreamCellOutput
  | DisplayDataCellOutput
  | ExecuteResultCellOutput
  | ErrorCellOutput

export interface BaseCellData {
  id: CellId
  type: CellType
  source: string
  metadata?: Record<string, unknown>
}

export interface CodeCellData extends BaseCellData {
  type: 'code'
  language: CellLanguage
  executionCount: number | null
  executionState: ExecutionState
  outputs: CellOutput[]
}

export interface MarkdownCellData extends BaseCellData {
  type: 'markdown'
}

export type CellData = CodeCellData | MarkdownCellData

export interface NotebookMetadata {
  title?: string
  kernelspec?: {
    display_name: string
    language: string
    name: string
  }
  language_info?: {
    name: string
    version?: string
  }
  [key: string]: unknown
}

export interface NotebookData {
  metadata: NotebookMetadata
  cells: CellData[]
}

export function createCodeCell(
  language: CellLanguage = 'python',
  source: string = ''
): CodeCellData {
  return {
    id: crypto.randomUUID(),
    type: 'code',
    language,
    source,
    executionCount: null,
    executionState: 'idle',
    outputs: [],
  }
}

export function createMarkdownCell(source: string = ''): MarkdownCellData {
  return {
    id: crypto.randomUUID(),
    type: 'markdown',
    source,
  }
}

export function createEmptyNotebook(): NotebookData {
  return {
    metadata: {},
    cells: [],
  }
}

export function isCodeCell(cell: CellData): cell is CodeCellData {
  return cell.type === 'code'
}

export function isMarkdownCell(cell: CellData): cell is MarkdownCellData {
  return cell.type === 'markdown'
}

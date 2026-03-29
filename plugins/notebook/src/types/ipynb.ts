// nbformat v4 types

export interface IpynbMimeBundle {
  'text/plain'?: string | string[]
  'text/html'?: string | string[]
  'text/markdown'?: string | string[]
  'image/png'?: string
  'image/svg+xml'?: string | string[]
  'image/jpeg'?: string
  'application/json'?: unknown
  [mimeType: string]: unknown
}

export interface IpynbStreamOutput {
  output_type: 'stream'
  name: 'stdout' | 'stderr'
  text: string | string[]
}

export interface IpynbDisplayData {
  output_type: 'display_data'
  data: IpynbMimeBundle
  metadata: Record<string, unknown>
}

export interface IpynbExecuteResult {
  output_type: 'execute_result'
  execution_count: number
  data: IpynbMimeBundle
  metadata: Record<string, unknown>
}

export interface IpynbError {
  output_type: 'error'
  ename: string
  evalue: string
  traceback: string[]
}

export type IpynbOutput =
  | IpynbStreamOutput
  | IpynbDisplayData
  | IpynbExecuteResult
  | IpynbError

export interface IpynbBaseCell {
  id?: string
  metadata: Record<string, unknown>
  source: string | string[]
}

export interface IpynbCodeCell extends IpynbBaseCell {
  cell_type: 'code'
  execution_count: number | null
  outputs: IpynbOutput[]
}

export interface IpynbMarkdownCell extends IpynbBaseCell {
  cell_type: 'markdown'
}

export interface IpynbRawCell extends IpynbBaseCell {
  cell_type: 'raw'
}

export type IpynbCell = IpynbCodeCell | IpynbMarkdownCell | IpynbRawCell

export interface IpynbNotebook {
  metadata: {
    kernelspec?: {
      display_name: string
      language: string
      name: string
    }
    language_info?: {
      name: string
      version?: string
      [key: string]: unknown
    }
    [key: string]: unknown
  }
  nbformat: 4
  nbformat_minor: number
  cells: IpynbCell[]
}

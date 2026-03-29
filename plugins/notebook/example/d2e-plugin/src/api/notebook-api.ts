import { request } from './request'
import type { NotebookRecord } from '../types'

export async function getNotebookList(datasetId: string): Promise<NotebookRecord[]> {
  const response = await request.get<NotebookRecord[]>('', {
    params: { datasetId },
  })
  return response.data
}

export async function createNotebook(
  datasetId: string,
  name: string,
  content: string
): Promise<NotebookRecord> {
  const response = await request.post<NotebookRecord>('', {
    name,
    notebookContent: content,
    datasetId,
  })
  return response.data
}

export async function saveNotebook(
  id: string,
  name: string,
  content: string,
  isShared: boolean,
  datasetId: string
): Promise<NotebookRecord> {
  const response = await request.put<NotebookRecord>('', {
    id,
    name,
    notebookContent: content,
    isShared,
    datasetId,
  })
  return response.data
}

export async function deleteNotebook(id: string, datasetId: string): Promise<void> {
  await request.delete(`/${id}`, {
    params: { datasetId },
  })
}

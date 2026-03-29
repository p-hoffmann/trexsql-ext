export interface PortalProps {
  appId?: string
  getToken?: () => Promise<string>
  username?: string
  idpUserId?: string
  datasetId?: string
  locale?: string
  containerId?: string
}

export interface NotebookRecord {
  id: string
  name: string
  notebookContent: string
  isShared: boolean
  datasetId: string
  userId?: string
}

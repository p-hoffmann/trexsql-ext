import { Notebook } from './components/notebook/Notebook'
import { PyodideKernel } from './kernels/pyodide/PyodideKernel'
import { WebRKernel } from './kernels/webr/WebRKernel'
import type { NotebookData } from './types/notebook'

const pyodideKernel = new PyodideKernel()
const webRKernel = new WebRKernel()

const initialNotebook: NotebookData = {
  metadata: {
    title: 'Demo Notebook',
  },
  cells: [
    {
      id: 'cell-1',
      type: 'code',
      language: 'python',
      source: '# Welcome to the notebook!\nprint("Hello, World!")',
      executionCount: null,
      executionState: 'idle',
      outputs: [],
    },
    {
      id: 'cell-2',
      type: 'code',
      language: 'python',
      source: 'import numpy as np\n\n# Create an array\narr = np.arange(10)\nprint(arr)',
      executionCount: null,
      executionState: 'idle',
      outputs: [],
    },
  ],
}

function App() {
  return (
    <div className="mx-auto max-w-4xl p-8">
      <h1 className="mb-6 text-2xl font-bold">React Notebook Demo</h1>
      <Notebook
        initialData={initialNotebook}
        onChange={(data) => console.log('Notebook changed:', data)}
        kernels={[pyodideKernel, webRKernel]}
        defaultKernelConfig={{ type: 'pyodide' }}
        showLineNumbers={true}
      />
    </div>
  )
}

export default App

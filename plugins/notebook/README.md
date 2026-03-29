# React Notebook

A lightweight, reusable React notebook component supporting Python, R, and Markdown cells with pluggable kernel backends.

## Features

- **Multi-language support**: Python (via Pyodide), R (via WebR), and Markdown cells
- **Pluggable kernels**: In-browser execution with Pyodide/WebR or remote Jupyter servers
- **Real-time output streaming**: See results as they're generated
- **Drag-and-drop cell reordering**: Intuitive cell management with @dnd-kit
- **Jupyter notebook compatibility**: Import/export .ipynb files
- **Undo/redo support**: Full history tracking for notebook operations
- **Keyboard shortcuts**: Jupyter-style shortcuts for efficient editing
- **Syntax highlighting**: CodeMirror 6 with Python and R support
- **Performance optimized**: Content-visibility virtualization for large notebooks

## Installation

```bash
npm install
```

## Quick Start

```tsx
import { Notebook, PyodideKernel, createEmptyNotebook } from 'react-notebook'

const kernel = new PyodideKernel()

function App() {
  return (
    <Notebook
      initialData={createEmptyNotebook()}
      kernels={[kernel]}
      defaultKernelConfig={{ type: 'pyodide' }}
      onChange={(data) => console.log('Notebook changed:', data)}
    />
  )
}
```

## Development

```bash
# Start development server
npm run dev

# Run unit tests
npm test

# Run tests with coverage
npm run test:coverage

# Run E2E tests
npm run test:e2e

# Type check
npm run typecheck

# Lint
npm run lint

# Production build
npm run build
```

## Project Structure

```
src/
├── components/notebook/   # React components (Notebook, Cell, CodeCell, etc.)
├── hooks/                 # React hooks (useNotebook, useKernel, useCellExecution)
├── kernels/               # Kernel implementations (Pyodide, WebR, Jupyter)
├── types/                 # TypeScript type definitions
└── utils/                 # Serialization utilities

example/                   # Demo application
tests/
├── unit/                  # Vitest unit tests
├── integration/           # Kernel integration tests
└── e2e/                   # Playwright E2E tests
```

## Kernel Configuration

### Pyodide (In-Browser Python)

```tsx
import { PyodideKernel } from 'react-notebook'

const kernel = new PyodideKernel()

// Connect with optional preloaded packages
await kernel.connect({
  type: 'pyodide',
  preloadPackages: ['numpy', 'pandas'],
})
```

### WebR (In-Browser R)

```tsx
import { WebRKernel } from 'react-notebook'

const kernel = new WebRKernel()

await kernel.connect({
  type: 'webr',
  preloadPackages: ['ggplot2'],
})
```

**Note**: WebR requires COOP/COEP headers for SharedArrayBuffer support:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

### Jupyter (Remote Server)

```tsx
import { JupyterKernel } from 'react-notebook'

const kernel = new JupyterKernel()

await kernel.connect({
  type: 'jupyter',
  serverUrl: 'http://localhost:8888',
  kernelId: 'your-kernel-id',
  token: 'your-token',
})
```

## Import/Export

```tsx
import { toIpynb, fromIpynb, serializeIpynb, parseIpynb } from 'react-notebook'

// Export to .ipynb format
const ipynb = toIpynb(notebookData)
const json = serializeIpynb(notebookData)

// Import from .ipynb
const data = fromIpynb(ipynbObject)
const data = parseIpynb(jsonString)
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Shift+Enter` | Run cell |
| `Ctrl/Cmd+Z` | Undo |
| `Ctrl/Cmd+Shift+Z` | Redo |
| `Arrow Up/Down` | Navigate cells |
| `A` | Add cell above |
| `B` | Add cell below |
| `M` | Convert to markdown |
| `Y` | Convert to code |
| `Delete/Backspace` | Delete cell |
| `Escape` | Deselect cell |

## API Reference

### NotebookProps

| Prop | Type | Description |
|------|------|-------------|
| `initialData` | `NotebookData` | Initial notebook data |
| `data` | `NotebookData` | Controlled notebook data |
| `onChange` | `(data: NotebookData) => void` | Called when notebook changes |
| `kernels` | `KernelPlugin[]` | Available kernel instances |
| `defaultKernelConfig` | `KernelConfig` | Auto-connect kernel config |
| `showToolbar` | `boolean` | Show/hide toolbar (default: true) |
| `showLineNumbers` | `boolean` | Show line numbers (default: true) |
| `readOnly` | `boolean` | Disable editing (default: false) |
| `virtualizationThreshold` | `number` | Cell count for virtualization (default: 50) |

### NotebookHandle (ref)

```tsx
const notebookRef = useRef<NotebookHandle>(null)

// Available methods
notebookRef.current?.getNotebookData()
notebookRef.current?.setNotebookData(data)
notebookRef.current?.addCell('code', position, 'python')
notebookRef.current?.deleteCell(cellId)
notebookRef.current?.moveCell(cellId, newPosition)
notebookRef.current?.runCell(cellId)
notebookRef.current?.runAllCells()
notebookRef.current?.interruptExecution()
notebookRef.current?.undo()
notebookRef.current?.redo()
```

## Tech Stack

- React 18 + TypeScript
- Vite
- CodeMirror 6 (via @uiw/react-codemirror)
- @dnd-kit for drag-and-drop
- Tailwind CSS + shadcn/ui
- Vitest + Playwright for testing

## License

MIT

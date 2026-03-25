export interface SelectedElement {
  devxId: string;       // "src/components/Button.tsx:15:4"
  devxName: string;     // "Button"
  tagName: string;      // "button"
  filePath: string;     // parsed from devxId
  line: number;         // parsed from devxId
  col: number;          // parsed from devxId
  boundingRect: { top: number; left: number; width: number; height: number };
}

export interface PendingChange {
  componentId: string;
  componentName: string;
  filePath: string;
  line: number;
  styles: StyleChanges;
  textContent?: string;
}

export interface StyleChanges {
  margin?: { top?: string; right?: string; bottom?: string; left?: string };
  padding?: { top?: string; right?: string; bottom?: string; left?: string };
  border?: { width?: string; radius?: string; color?: string };
  backgroundColor?: string;
  text?: { fontSize?: string; fontWeight?: string; color?: string };
}

export interface VisualEditContext {
  filePath: string;
  line: number;
  componentName: string;
}

export interface SelectedComponent {
  devxId: string;       // "src/components/Button.tsx:15:4"
  devxName: string;     // "Button"
  filePath: string;
  line: number;
}

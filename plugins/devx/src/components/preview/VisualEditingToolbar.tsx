import { useState, useCallback, useEffect } from "react";
import { X, Type, RotateCcw, Save, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { SelectedElement, PendingChange, StyleChanges } from "@/lib/visual-editing-types";

interface VisualEditingToolbarProps {
  element: SelectedElement;
  iframeRef: React.RefObject<HTMLIFrameElement | null>;
  onEditWithAI: (element: SelectedElement) => void;
  onSave: (changes: PendingChange[]) => void;
  onClose: () => void;
}

interface ComputedStyles {
  marginTop: string;
  marginRight: string;
  marginBottom: string;
  marginLeft: string;
  paddingTop: string;
  paddingRight: string;
  paddingBottom: string;
  paddingLeft: string;
  borderWidth: string;
  borderRadius: string;
  borderColor: string;
  backgroundColor: string;
  fontSize: string;
  fontWeight: string;
  color: string;
}

export function VisualEditingToolbar({
  element,
  iframeRef,
  onEditWithAI,
  onSave,
  onClose,
}: VisualEditingToolbarProps) {
  const [computedStyles, setComputedStyles] = useState<ComputedStyles | null>(null);
  const [pendingStyles, setPendingStyles] = useState<StyleChanges>({});
  const [pendingText, setPendingText] = useState<string | undefined>(undefined);
  const [changedFields, setChangedFields] = useState<Set<string>>(new Set());

  // Request computed styles from iframe on mount
  useEffect(() => {
    const iframe = iframeRef.current;
    if (!iframe?.contentWindow) return;

    iframe.contentWindow.postMessage(
      { type: "get-devx-component-styles", devxId: element.devxId },
      "*",
    );

    function handleMessage(e: MessageEvent) {
      // Only accept messages from our iframe
      if (e.source !== iframe?.contentWindow) return;
      if (e.data?.type === "devx-component-styles" && e.data.devxId === element.devxId) {
        setComputedStyles(e.data.styles);
      }
      if (e.data?.type === "devx-text-finalized" && e.data.devxId === element.devxId) {
        setPendingText(e.data.textContent);
        setChangedFields((prev) => new Set(prev).add("textContent"));
      }
    }

    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  }, [element.devxId, iframeRef]);

  const sendStyleUpdate = useCallback(
    (styles: Record<string, string>) => {
      const iframe = iframeRef.current;
      if (!iframe?.contentWindow) return;
      iframe.contentWindow.postMessage(
        { type: "modify-devx-component-styles", devxId: element.devxId, styles },
        "*",
      );
    },
    [element.devxId, iframeRef],
  );

  const updateSpacing = useCallback(
    (type: "margin" | "padding", dir: "top" | "right" | "bottom" | "left", value: string) => {
      setPendingStyles((prev) => ({
        ...prev,
        [type]: { ...prev[type], [dir]: value },
      }));
      const cssProp = `${type}${dir.charAt(0).toUpperCase() + dir.slice(1)}`;
      sendStyleUpdate({ [cssProp]: value });
      setChangedFields((prev) => new Set(prev).add(`${type}.${dir}`));
    },
    [sendStyleUpdate],
  );

  const updateBorder = useCallback(
    (prop: "width" | "radius" | "color", value: string) => {
      setPendingStyles((prev) => ({
        ...prev,
        border: { ...prev.border, [prop]: value },
      }));
      const cssMap = { width: "borderWidth", radius: "borderRadius", color: "borderColor" };
      sendStyleUpdate({ [cssMap[prop]]: value });
      setChangedFields((prev) => new Set(prev).add(`border.${prop}`));
    },
    [sendStyleUpdate],
  );

  const updateBackground = useCallback(
    (value: string) => {
      setPendingStyles((prev) => ({ ...prev, backgroundColor: value }));
      sendStyleUpdate({ backgroundColor: value });
      setChangedFields((prev) => new Set(prev).add("backgroundColor"));
    },
    [sendStyleUpdate],
  );

  const updateText = useCallback(
    (prop: "fontSize" | "fontWeight" | "color", value: string) => {
      setPendingStyles((prev) => ({
        ...prev,
        text: { ...prev.text, [prop]: value },
      }));
      sendStyleUpdate({ [prop]: value });
      setChangedFields((prev) => new Set(prev).add(`text.${prop}`));
    },
    [sendStyleUpdate],
  );

  const handleEnableTextEdit = useCallback(() => {
    const iframe = iframeRef.current;
    if (!iframe?.contentWindow) return;
    iframe.contentWindow.postMessage(
      { type: "enable-devx-text-editing", devxId: element.devxId },
      "*",
    );
  }, [element.devxId, iframeRef]);

  const handleReset = useCallback(() => {
    const iframe = iframeRef.current;
    if (!iframe?.contentWindow) return;
    iframe.contentWindow.postMessage(
      { type: "reset-devx-component-styles", devxId: element.devxId },
      "*",
    );
    setPendingStyles({});
    setPendingText(undefined);
    setChangedFields(new Set());
  }, [element.devxId, iframeRef]);

  const handleSave = useCallback(() => {
    const change: PendingChange = {
      componentId: element.devxId,
      componentName: element.devxName,
      filePath: element.filePath,
      line: element.line,
      styles: pendingStyles,
      textContent: pendingText,
    };
    onSave([change]);
  }, [element, pendingStyles, pendingText, onSave]);

  const spacingValue = (type: "margin" | "padding", dir: string): string => {
    const pending = pendingStyles[type];
    if (pending && pending[dir as keyof typeof pending]) {
      return pending[dir as keyof typeof pending]!;
    }
    if (computedStyles) {
      const key = `${type}${dir.charAt(0).toUpperCase() + dir.slice(1)}` as keyof ComputedStyles;
      return computedStyles[key] || "";
    }
    return "";
  };

  return (
    <div className="absolute right-2 top-12 z-20 w-64 bg-background border rounded-lg shadow-xl text-xs overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b bg-muted/50">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className="font-medium text-primary truncate">&lt;{element.devxName}&gt;</span>
        </div>
        <div className="flex items-center gap-1 shrink-0">
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6"
            title="Edit with AI"
            onClick={() => onEditWithAI(element)}
          >
            <Sparkles className="h-3 w-3" />
          </Button>
          <Button variant="ghost" size="icon" className="h-6 w-6" onClick={onClose}>
            <X className="h-3 w-3" />
          </Button>
        </div>
      </div>

      <div className="p-3 space-y-3 max-h-[400px] overflow-y-auto">
        {/* File path */}
        <div className="text-muted-foreground truncate" title={element.filePath}>
          {element.filePath}:{element.line}
        </div>

        {/* Spacing: Margin */}
        <div>
          <Label className="text-[10px] uppercase text-muted-foreground">Margin</Label>
          <div className="grid grid-cols-4 gap-1 mt-1">
            {(["top", "right", "bottom", "left"] as const).map((dir) => (
              <div key={dir}>
                <Input
                  className="h-6 text-[10px] px-1 text-center"
                  placeholder={dir[0].toUpperCase()}
                  value={spacingValue("margin", dir)}
                  onChange={(e) => updateSpacing("margin", dir, e.target.value)}
                />
              </div>
            ))}
          </div>
        </div>

        {/* Spacing: Padding */}
        <div>
          <Label className="text-[10px] uppercase text-muted-foreground">Padding</Label>
          <div className="grid grid-cols-4 gap-1 mt-1">
            {(["top", "right", "bottom", "left"] as const).map((dir) => (
              <div key={dir}>
                <Input
                  className="h-6 text-[10px] px-1 text-center"
                  placeholder={dir[0].toUpperCase()}
                  value={spacingValue("padding", dir)}
                  onChange={(e) => updateSpacing("padding", dir, e.target.value)}
                />
              </div>
            ))}
          </div>
        </div>

        {/* Border */}
        <div>
          <Label className="text-[10px] uppercase text-muted-foreground">Border</Label>
          <div className="grid grid-cols-3 gap-1 mt-1">
            <Input
              className="h-6 text-[10px] px-1"
              placeholder="Width"
              value={pendingStyles.border?.width || computedStyles?.borderWidth || ""}
              onChange={(e) => updateBorder("width", e.target.value)}
            />
            <Input
              className="h-6 text-[10px] px-1"
              placeholder="Radius"
              value={pendingStyles.border?.radius || computedStyles?.borderRadius || ""}
              onChange={(e) => updateBorder("radius", e.target.value)}
            />
            <Input
              type="color"
              className="h-6 w-full px-0.5 cursor-pointer"
              value={pendingStyles.border?.color || "#000000"}
              onChange={(e) => updateBorder("color", e.target.value)}
            />
          </div>
        </div>

        {/* Background */}
        <div>
          <Label className="text-[10px] uppercase text-muted-foreground">Background</Label>
          <div className="flex gap-1 mt-1">
            <Input
              type="color"
              className="h-6 w-8 px-0.5 cursor-pointer shrink-0"
              value={pendingStyles.backgroundColor || "#ffffff"}
              onChange={(e) => updateBackground(e.target.value)}
            />
            <Input
              className="h-6 text-[10px] px-1 flex-1"
              placeholder="Color value"
              value={pendingStyles.backgroundColor || computedStyles?.backgroundColor || ""}
              onChange={(e) => updateBackground(e.target.value)}
            />
          </div>
        </div>

        {/* Text */}
        <div>
          <div className="flex items-center justify-between">
            <Label className="text-[10px] uppercase text-muted-foreground">Text</Label>
            <Button
              variant="ghost"
              size="icon"
              className="h-5 w-5"
              title="Edit text content"
              onClick={handleEnableTextEdit}
            >
              <Type className="h-3 w-3" />
            </Button>
          </div>
          <div className="grid grid-cols-3 gap-1 mt-1">
            <Input
              className="h-6 text-[10px] px-1"
              placeholder="Size"
              value={pendingStyles.text?.fontSize || computedStyles?.fontSize || ""}
              onChange={(e) => updateText("fontSize", e.target.value)}
            />
            <select
              className="h-6 text-[10px] px-0.5 border rounded bg-background"
              value={pendingStyles.text?.fontWeight || computedStyles?.fontWeight || "400"}
              onChange={(e) => updateText("fontWeight", e.target.value)}
            >
              <option value="100">Thin</option>
              <option value="300">Light</option>
              <option value="400">Normal</option>
              <option value="500">Medium</option>
              <option value="600">Semi</option>
              <option value="700">Bold</option>
              <option value="900">Black</option>
            </select>
            <Input
              type="color"
              className="h-6 w-full px-0.5 cursor-pointer"
              value={pendingStyles.text?.color || "#000000"}
              onChange={(e) => updateText("color", e.target.value)}
            />
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between px-3 py-2 border-t bg-muted/50">
        <Button variant="ghost" size="sm" className="h-6 text-[10px] gap-1" onClick={handleReset}>
          <RotateCcw className="h-3 w-3" />
          Reset
        </Button>
        <div className="flex items-center gap-1.5">
          {changedFields.size > 0 && (
            <span className="text-[10px] text-muted-foreground">{changedFields.size} changes</span>
          )}
          <Button
            size="sm"
            className="h-6 text-[10px] gap-1"
            onClick={handleSave}
            disabled={changedFields.size === 0}
          >
            <Save className="h-3 w-3" />
            Save
          </Button>
        </div>
      </div>
    </div>
  );
}

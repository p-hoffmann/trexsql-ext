import { lazy, Suspense, useState, useEffect, useCallback } from "react";
import { Save, Check, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { Extension } from "@codemirror/state";
import { EditorView } from "@codemirror/view";

const ReactCodeMirror = lazy(() => import("@uiw/react-codemirror"));

const lightTheme = EditorView.theme({
  "&": { backgroundColor: "hsl(var(--background))", color: "hsl(var(--foreground))" },
  ".cm-gutters": { backgroundColor: "hsl(var(--muted))", color: "hsl(var(--muted-foreground))", borderRight: "1px solid hsl(var(--border))" },
  ".cm-activeLineGutter": { backgroundColor: "hsl(var(--accent))" },
  ".cm-activeLine": { backgroundColor: "hsl(var(--accent) / 0.5)" },
  ".cm-selectionBackground, ::selection": { backgroundColor: "hsl(var(--accent))" },
  ".cm-cursor": { borderLeftColor: "hsl(var(--foreground))" },
}, { dark: false });

const darkTheme = EditorView.theme({
  "&": { backgroundColor: "hsl(var(--background))", color: "hsl(var(--foreground))" },
  ".cm-gutters": { backgroundColor: "hsl(var(--muted))", color: "hsl(var(--muted-foreground))", borderRight: "1px solid hsl(var(--border))" },
  ".cm-activeLineGutter": { backgroundColor: "hsl(var(--accent))" },
  ".cm-activeLine": { backgroundColor: "hsl(var(--accent) / 0.5)" },
  ".cm-selectionBackground, ::selection": { backgroundColor: "hsl(var(--accent))" },
  ".cm-cursor": { borderLeftColor: "hsl(var(--foreground))" },
}, { dark: true });

// Map file extensions to language extension loaders
const langLoaders: Record<string, () => Promise<Extension>> = {
  ts: () => import("@codemirror/lang-javascript").then((m) => m.javascript({ typescript: true, jsx: true })),
  tsx: () => import("@codemirror/lang-javascript").then((m) => m.javascript({ typescript: true, jsx: true })),
  js: () => import("@codemirror/lang-javascript").then((m) => m.javascript({ jsx: true })),
  jsx: () => import("@codemirror/lang-javascript").then((m) => m.javascript({ jsx: true })),
  mjs: () => import("@codemirror/lang-javascript").then((m) => m.javascript()),
  html: () => import("@codemirror/lang-html").then((m) => m.html()),
  css: () => import("@codemirror/lang-css").then((m) => m.css()),
  scss: () => import("@codemirror/lang-css").then((m) => m.css()),
  json: () => import("@codemirror/lang-json").then((m) => m.json()),
};

interface CodeViewerProps {
  content: string;
  filePath: string;
  onSave?: (filePath: string, content: string) => Promise<void>;
}

function CodeViewerInner({ content, filePath, onSave }: CodeViewerProps) {
  const ext = filePath.split(".").pop() || "";
  const [langExt, setLangExt] = useState<Extension[]>([]);
  const [editedContent, setEditedContent] = useState(content);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [isDark, setIsDark] = useState(() => document.documentElement.classList.contains("dark"));

  const isModified = editedContent !== content;

  // Watch for theme changes
  useEffect(() => {
    const observer = new MutationObserver(() => {
      setIsDark(document.documentElement.classList.contains("dark"));
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, []);

  // Reset edited content when the file changes
  useEffect(() => {
    setEditedContent(content);
    setSaved(false);
  }, [content, filePath]);

  useEffect(() => {
    const loader = langLoaders[ext];
    if (loader) {
      loader().then((e) => setLangExt([e]));
    } else {
      setLangExt([]);
    }
  }, [ext]);

  const handleSave = useCallback(async () => {
    if (!onSave || !isModified) return;
    setSaving(true);
    try {
      await onSave(filePath, editedContent);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err) {
      console.error("Failed to save file:", err);
    } finally {
      setSaving(false);
    }
  }, [onSave, filePath, editedContent, isModified]);

  const breadcrumbs = filePath.split("/").filter(Boolean);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-3 py-1 border-b bg-muted/30">
        <div className="flex items-center gap-0.5 text-xs text-muted-foreground min-w-0 overflow-hidden">
          {breadcrumbs.map((segment, i) => (
            <span key={i} className="flex items-center gap-0.5 shrink-0">
              {i > 0 && <ChevronRight className="h-3 w-3 opacity-40" />}
              <span className={i === breadcrumbs.length - 1 ? "font-semibold text-foreground" : ""}>
                {segment}
              </span>
            </span>
          ))}
        </div>
        {onSave && (
          <Button
            size="sm"
            variant="ghost"
            className="h-6 text-xs gap-1 shrink-0"
            onClick={handleSave}
            disabled={!isModified || saving}
          >
            {saved ? (
              <>
                <Check className="h-3 w-3" />
                Saved!
              </>
            ) : (
              <>
                <Save className="h-3 w-3" />
                {saving ? "Saving..." : "Save"}
              </>
            )}
          </Button>
        )}
      </div>
      <div className="flex-1 overflow-auto">
        <ReactCodeMirror
          value={editedContent}
          onChange={(value) => setEditedContent(value)}
          basicSetup={{
            lineNumbers: true,
            foldGutter: true,
            highlightActiveLine: true,
          }}
          extensions={[...langExt, isDark ? darkTheme : lightTheme]}
          className="h-full text-xs"
          theme={isDark ? "dark" : "light"}
        />
      </div>
    </div>
  );
}

export function CodeViewer({ content, filePath, onSave }: CodeViewerProps) {
  return (
    <Suspense
      fallback={
        <div className="p-4 text-xs text-muted-foreground">Loading editor...</div>
      }
    >
      <CodeViewerInner content={content} filePath={filePath} onSave={onSave} />
    </Suspense>
  );
}

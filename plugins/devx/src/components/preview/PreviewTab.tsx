import { useState, useRef, useCallback, useEffect } from "react";
import {
  PanelGroup,
  Panel,
  PanelResizeHandle,
} from "react-resizable-panels";
import type { ImperativePanelHandle } from "react-resizable-panels";
import { RefreshCw, Smartphone, Tablet, Monitor, Play, Square, MousePointer2, Terminal, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { API_BASE } from "@/lib/config";
import type { useDevServer } from "@/hooks/useDevServer";
import { PreviewAnnotator } from "./PreviewAnnotator";
import { VisualEditingToolbar } from "./VisualEditingToolbar";
import { VisualEditingChangesDialog } from "./VisualEditingChangesDialog";
import { ConsoleTab } from "./ConsoleTab";
import type { SelectedElement, SelectedComponent, PendingChange } from "@/lib/visual-editing-types";
import * as api from "@/lib/api";
import { PreviewConfigBar } from "./PreviewConfigBar";
import type { App } from "@/lib/types";

const VIEWPORT_PRESETS = [
  { label: "Mobile", icon: Smartphone, width: 375 },
  { label: "Tablet", icon: Tablet, width: 768 },
  { label: "Desktop", icon: Monitor, width: 0 }, // 0 = 100%
];

interface PreviewTabProps {
  appId: string;
  app?: App | null;
  devServer: ReturnType<typeof useDevServer>;
  onEditWithAI?: (element: SelectedElement) => void;
  onComponentsSelected?: (components: SelectedComponent[]) => void;
  refreshSignal?: number;
  appConfig?: Record<string, string> | null;
  onConfigChanged?: (config: Record<string, string>) => void;
}

export function PreviewTab({ appId, app, devServer, onEditWithAI, onComponentsSelected, refreshSignal, appConfig, onConfigChanged }: PreviewTabProps) {
  const { status, start, stop } = devServer;
  const [viewportWidth, setViewportWidth] = useState(0);
  const [annotatorEnabled, setAnnotatorEnabled] = useState(false);
  const [multiSelectEnabled, setMultiSelectEnabled] = useState(false);
  const [selectedElement, setSelectedElement] = useState<SelectedElement | null>(null);
  const [pendingChanges, setPendingChanges] = useState<PendingChange[] | null>(null);
  const [consoleVisible, setConsoleVisible] = useState(false);
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const consolePanelRef = useRef<ImperativePanelHandle>(null);

  const isRunning = status.status === "running";
  const isStarting = status.status === "starting";
  const token = (() => {
    try {
      const raw = localStorage.getItem("trex.auth.session");
      if (raw) return JSON.parse(raw).access_token;
    } catch { /* ignore */ }
    return null;
  })();
  const proxyUrl = (() => {
    const params = new URLSearchParams();
    if (token) params.set("token", token);
    if (appConfig) {
      for (const [k, v] of Object.entries(appConfig)) {
        if (v) params.set(k, v);
      }
    }
    const qs = params.toString();
    return `${API_BASE}/apps/${appId}/proxy/${qs ? `?${qs}` : ""}`;
  })();

  const handleRefresh = useCallback(() => {
    if (iframeRef.current) {
      iframeRef.current.src = iframeRef.current.src;
    }
  }, []);

  // Refresh iframe when refreshSignal changes (triggered by agent's refresh_app_preview tool)
  useEffect(() => {
    if (refreshSignal && refreshSignal > 0) {
      handleRefresh();
    }
  }, [refreshSignal, handleRefresh]);

  const toggleConsole = useCallback(() => {
    const panel = consolePanelRef.current;
    if (!panel) return;
    if (consoleVisible) panel.collapse();
    else panel.expand();
  }, [consoleVisible]);

  const handleSelectElement = useCallback((element: SelectedElement) => {
    setSelectedElement(element);
  }, []);

  const handleCloseAnnotator = useCallback(() => {
    setAnnotatorEnabled(false);
    setSelectedElement(null);
  }, []);

  const handleCloseToolbar = useCallback(() => {
    setSelectedElement(null);
  }, []);

  const handleEditWithAI = useCallback(
    (element: SelectedElement) => {
      onEditWithAI?.(element);
    },
    [onEditWithAI],
  );

  // Multi-select mode: activate/deactivate and listen for messages
  useEffect(() => {
    if (!multiSelectEnabled) return;
    const iframe = iframeRef.current;
    if (!iframe?.contentWindow) return;

    const contentWindow = iframe.contentWindow;
    const timer = setTimeout(() => {
      contentWindow.postMessage({ type: "activate-devx-multi-selector" }, "*");
    }, 100);

    function handleMessage(e: MessageEvent) {
      if (e.source !== contentWindow) return;
      if (e.data?.type === "devx-components-selected") {
        const components: SelectedComponent[] = (e.data.components || []).map(
          (c: { devxId: string; devxName: string }) => {
            const parts = c.devxId.split(":");
            parts.pop(); // col
            const line = parseInt(parts.pop() || "0", 10);
            const filePath = parts.join(":");
            return { devxId: c.devxId, devxName: c.devxName, filePath, line };
          },
        );
        onComponentsSelected?.(components);
      }
      if (e.data?.type === "devx-multi-selector-closed") {
        setMultiSelectEnabled(false);
        onComponentsSelected?.([]);
      }
    }

    window.addEventListener("message", handleMessage);
    return () => {
      clearTimeout(timer);
      window.removeEventListener("message", handleMessage);
      try {
        contentWindow.postMessage({ type: "deactivate-devx-multi-selector" }, "*");
      } catch { /* iframe may be destroyed */ }
    };
  }, [multiSelectEnabled, onComponentsSelected]);

  const handleSaveChanges = useCallback((changes: PendingChange[]) => {
    setPendingChanges(changes);
  }, []);

  const handleConfirmSave = useCallback(async () => {
    if (!pendingChanges) return;
    try {
      await api.applyVisualEdit(appId, pendingChanges);
      setPendingChanges(null);
      setSelectedElement(null);
      // Refresh iframe to show updated code
      handleRefresh();
    } catch (err) {
      console.error("Failed to save visual changes:", err);
    }
  }, [appId, pendingChanges]);

  const handleCancelSave = useCallback(() => {
    setPendingChanges(null);
  }, []);

  return (
    <PanelGroup direction="vertical">
      <Panel defaultSize={75} minSize={20} id="preview">
        <div className="flex flex-col h-full">
          {/* Controls bar */}
          <div className="flex items-center gap-2 px-3 py-1.5 border-b shrink-0">
            {!isRunning && !isStarting ? (
              <Button variant="ghost" size="sm" className="gap-1.5 h-7 text-xs" onClick={start}>
                <Play className="h-3 w-3" />
                Start
              </Button>
            ) : (
              <Button variant="ghost" size="sm" className="gap-1.5 h-7 text-xs" onClick={stop}>
                <Square className="h-3 w-3" />
                Stop
              </Button>
            )}

            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={handleRefresh} disabled={!isRunning}>
              <RefreshCw className="h-3.5 w-3.5" />
            </Button>

            {/* Status indicator */}
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <div
                className={`h-2 w-2 rounded-full ${
                  isRunning ? "bg-green-500" : isStarting ? "bg-yellow-500 animate-pulse" : "bg-gray-400"
                }`}
              />
              <span>
                {isRunning && (status.url || status.port)
                  ? status.url
                    ? new URL(status.url).host
                    : `localhost:${status.port}`
                  : status.status}
              </span>
            </div>

            <div className="flex-1" />

            {/* Settings popover */}
            {app && onConfigChanged && (
              <PreviewConfigBar app={app} onConfigChanged={onConfigChanged} />
            )}

            {/* Console toggle */}
            <Button
              variant={consoleVisible ? "secondary" : "ghost"}
              size="icon"
              className="h-7 w-7"
              title="Toggle console"
              onClick={toggleConsole}
            >
              <Terminal className="h-3.5 w-3.5" />
            </Button>

            {/* AI Select toggle */}
            <Button
              variant={multiSelectEnabled ? "secondary" : "ghost"}
              size="icon"
              className="h-7 w-7"
              title="Select components for AI"
              onClick={() => {
                const next = !multiSelectEnabled;
                setMultiSelectEnabled(next);
                if (next && annotatorEnabled) {
                  setAnnotatorEnabled(false);
                  setSelectedElement(null);
                }
                if (!next) {
                  onComponentsSelected?.([]);
                }
              }}
              disabled={!isRunning}
            >
              <Sparkles className="h-3.5 w-3.5" />
            </Button>

            {/* Annotator toggle */}
            <Button
              variant={annotatorEnabled ? "secondary" : "ghost"}
              size="icon"
              className="h-7 w-7"
              title="Visual editing mode"
              onClick={() => {
                const next = !annotatorEnabled;
                setAnnotatorEnabled(next);
                if (next && multiSelectEnabled) {
                  setMultiSelectEnabled(false);
                  onComponentsSelected?.([]);
                }
                if (!next) {
                  setSelectedElement(null);
                }
              }}
              disabled={!isRunning}
            >
              <MousePointer2 className="h-3.5 w-3.5" />
            </Button>

            {/* Viewport dropdown */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="h-7 w-7" title="Viewport size">
                  {(() => {
                    const ActiveIcon = VIEWPORT_PRESETS.find((p) => p.width === viewportWidth)?.icon || Monitor;
                    return <ActiveIcon className="h-3.5 w-3.5" />;
                  })()}
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {VIEWPORT_PRESETS.map((preset) => (
                  <DropdownMenuItem
                    key={preset.label}
                    onClick={() => setViewportWidth(preset.width)}
                    className="gap-2 text-xs"
                  >
                    <preset.icon className="h-3.5 w-3.5" />
                    {preset.label}
                    {preset.width > 0 && <span className="text-muted-foreground ml-auto">{preset.width}px</span>}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          {/* iframe */}
          <div className="flex-1 flex items-start justify-center bg-muted/30 overflow-auto relative">
            {isRunning ? (
              <>
                <iframe
                  ref={iframeRef}
                  src={proxyUrl}
                  className="bg-white border-x border-b"
                  style={{
                    width: viewportWidth > 0 ? `${viewportWidth}px` : "100%",
                    height: "100%",
                  }}
                  title="App Preview"
                />
                {multiSelectEnabled && (
                  <div className="absolute top-2 right-2 z-20 flex items-center gap-2">
                    <div className="bg-background/90 border rounded-lg px-3 py-1.5 text-xs flex items-center gap-2 shadow-lg">
                      <Sparkles className="h-3.5 w-3.5 text-indigo-500" />
                      Click components to select for AI
                    </div>
                  </div>
                )}
                {annotatorEnabled && (
                  <PreviewAnnotator
                    iframeRef={iframeRef}
                    onSelectElement={handleSelectElement}
                    onClose={handleCloseAnnotator}
                  />
                )}
                {selectedElement && (
                  <VisualEditingToolbar
                    element={selectedElement}
                    iframeRef={iframeRef}
                    onEditWithAI={handleEditWithAI}
                    onSave={handleSaveChanges}
                    onClose={handleCloseToolbar}
                  />
                )}
                {pendingChanges && (
                  <VisualEditingChangesDialog
                    changes={pendingChanges}
                    onConfirm={handleConfirmSave}
                    onCancel={handleCancelSave}
                  />
                )}
              </>
            ) : (
              <div className="flex h-full w-full items-center justify-center text-muted-foreground">
                <div className="text-center space-y-3">
                  <Monitor className="h-12 w-12 mx-auto opacity-30" />
                  <p className="text-sm">
                    {isStarting ? "Starting dev server..." : "Dev server is not running"}
                  </p>
                  {!isStarting && (
                    <Button size="sm" onClick={start}>
                      <Play className="h-3.5 w-3.5 mr-1.5" />
                      Start Dev Server
                    </Button>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </Panel>
      <PanelResizeHandle className="h-1 bg-border hover:bg-primary/20 cursor-row-resize" />
      <Panel
        ref={consolePanelRef}
        defaultSize={25}
        minSize={10}
        collapsible
        onCollapse={() => setConsoleVisible(false)}
        onExpand={() => setConsoleVisible(true)}
        id="console"
      >
        <ConsoleTab devServer={devServer} />
      </Panel>
    </PanelGroup>
  );
}

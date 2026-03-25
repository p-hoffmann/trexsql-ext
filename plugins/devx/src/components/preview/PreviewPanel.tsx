import { useState, useEffect, useCallback } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Monitor, Code, AlertTriangle, GitBranch, ClipboardList, Package } from "lucide-react";
import { PreviewTab } from "./PreviewTab";
import { CodeTab } from "./CodeTab";
import { ProblemsTab } from "./ProblemsTab";
import { GitTab } from "./GitTab";
import { PlanTab } from "./PlanTab";
import { PublishTab } from "./PublishTab";
import { useFileTree } from "@/hooks/useFileTree";
import { useDevServer } from "@/hooks/useDevServer";
import { useGit } from "@/hooks/useGit";
import type { App } from "@/lib/types";
import * as api from "@/lib/api";

interface PreviewPanelProps {
  appId: string | null;
  planContent?: string | null;
  chatMode?: string;
  onEditWithAI?: (element: import("@/lib/visual-editing-types").SelectedElement) => void;
  onComponentsSelected?: (components: import("@/lib/visual-editing-types").SelectedComponent[]) => void;
  refreshSignal?: number;
  onFixPrompt?: (prompt: string) => void;
}

export function PreviewPanel({ appId, planContent, chatMode, onEditWithAI, onComponentsSelected, refreshSignal, onFixPrompt }: PreviewPanelProps) {
  const [activeTab, setActiveTab] = useState("preview");
  const [app, setApp] = useState<App | null>(null);
  const [configRefresh, setConfigRefresh] = useState(0);
  const fileTree = useFileTree(appId);
  const devServer = useDevServer(appId);
  const git = useGit(appId);

  useEffect(() => {
    if (appId) {
      api.getApp(appId).then(setApp).catch(() => {});
    } else {
      setApp(null);
    }
  }, [appId]);

  const handleConfigChanged = useCallback(() => {
    // Refresh the app data and trigger a preview refresh
    if (appId) {
      api.getApp(appId).then(setApp).catch(() => {});
    }
    setConfigRefresh((n) => n + 1);
  }, [appId]);

  useEffect(() => {
    if (refreshSignal && refreshSignal > 0) {
      fileTree.refresh();
      fileTree.reloadSelectedFile();
    }
  }, [refreshSignal]);

  if (!appId && chatMode !== "plan" && !planContent) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        <div className="text-center space-y-2">
          <Monitor className="h-10 w-10 mx-auto opacity-40" />
          <p className="text-sm">Select an app to preview</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <Tabs value={activeTab} onValueChange={setActiveTab} className="flex flex-col h-full">
        <TabsList className="w-full justify-start rounded-none border-b bg-transparent px-2 h-9 shrink-0">
          <TabsTrigger value="preview" className="gap-1.5 text-xs">
            <Monitor className="h-3.5 w-3.5" />
            Preview
          </TabsTrigger>
          <TabsTrigger value="code" className="gap-1.5 text-xs">
            <Code className="h-3.5 w-3.5" />
            Code
          </TabsTrigger>
          <TabsTrigger value="problems" className="gap-1.5 text-xs">
            <AlertTriangle className="h-3.5 w-3.5" />
            Checks
          </TabsTrigger>
          {(chatMode === "plan" || planContent) && (
            <TabsTrigger value="plan" className="gap-1.5 text-xs">
              <ClipboardList className="h-3.5 w-3.5" />
              Plan
            </TabsTrigger>
          )}
          <TabsTrigger value="git" className="gap-1.5 text-xs">
            <GitBranch className="h-3.5 w-3.5" />
            Git
            {git.status.length > 0 && (
              <span className="ml-1 text-[10px] bg-yellow-500/20 text-yellow-600 px-1 rounded">
                {git.status.length}
              </span>
            )}
          </TabsTrigger>
          <TabsTrigger value="publish" className="gap-1.5 text-xs">
            <Package className="h-3.5 w-3.5" />
            Export
          </TabsTrigger>
        </TabsList>

        {appId && (
          <>
            <TabsContent value="preview" className="flex-1 m-0 overflow-hidden">
              <PreviewTab appId={appId} app={app} devServer={devServer} onEditWithAI={onEditWithAI} onComponentsSelected={onComponentsSelected} refreshSignal={(refreshSignal || 0) + configRefresh} appConfig={app?.config} onConfigChanged={handleConfigChanged} />
            </TabsContent>
            <TabsContent value="code" className="flex-1 m-0 overflow-hidden">
              <CodeTab fileTree={fileTree} />
            </TabsContent>
            <TabsContent value="problems" className="flex-1 m-0 overflow-hidden">
              <ProblemsTab
                appId={appId}
                onOpenFile={(path) => {
                  fileTree.selectFile(path);
                  setActiveTab("code");
                }}
                onFixPrompt={onFixPrompt}
              />
            </TabsContent>
            <TabsContent value="git" className="flex-1 m-0 overflow-hidden">
              <GitTab git={git} appId={appId} />
            </TabsContent>
            <TabsContent value="publish" className="flex-1 m-0 overflow-hidden">
              <PublishTab appId={appId} />
            </TabsContent>
          </>
        )}
        <TabsContent value="plan" className="flex-1 m-0 overflow-hidden">
          <PlanTab content={planContent ?? null} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

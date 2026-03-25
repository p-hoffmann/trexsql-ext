import { useState, useCallback, useMemo, useRef } from "react";
import { Link } from "react-router-dom";
import {
  PanelGroup,
  Panel,
  PanelResizeHandle,
} from "react-resizable-panels";
import type { ImperativePanelHandle } from "react-resizable-panels";
import { Settings, PanelLeftClose, PanelLeftOpen, PanelRightClose, PanelRightOpen } from "lucide-react";
import { ChatPanel } from "@/components/ChatPanel";
import { ChatSidebar } from "@/components/chat/ChatSidebar";
import { PreviewPanel } from "@/components/preview/PreviewPanel";
import { AppSelector } from "@/components/AppSelector";
import { ThemeToggle } from "@/components/ThemeToggle";
import { Button } from "@/components/ui/button";
import { useChats } from "@/hooks/useChats";
import { useApps } from "@/hooks/useApps";
import { useSettings } from "@/hooks/useSettings";
import { useKeyboardShortcuts } from "@/hooks/useKeyboardShortcuts";
import { cn } from "@/lib/utils";
import type { ChatMode } from "@/lib/types";
import type { SelectedElement, SelectedComponent, VisualEditContext } from "@/lib/visual-editing-types";

export default function ChatPage() {
  const [activeChatId, setActiveChatId] = useState<string | null>(null);
  const [activeAppId, setActiveAppId] = useState<string | null>(null);
  const { chats, create, remove, updateMode } = useChats(activeAppId);
  const { apps, create: createApp, remove: removeApp } = useApps();
  useSettings(); // pre-load settings for navigation to settings page
  const [isResizing, setIsResizing] = useState(false);
  const [modeOverride, setModeOverride] = useState<ChatMode | null>(null);
  const [planContent, setPlanContent] = useState<string | null>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [visualEditContext, setVisualEditContext] = useState<VisualEditContext | null>(null);
  const [selectedComponents, setSelectedComponents] = useState<SelectedComponent[]>([]);
  const [refreshSignal, setRefreshSignal] = useState(0);
  const sendRef = useRef<((msg: string) => void) | null>(null);
  const chatPanelRef = useRef<ImperativePanelHandle>(null);
  const previewPanelRef = useRef<ImperativePanelHandle>(null);
  const [chatCollapsed, setChatCollapsed] = useState(false);
  const [previewCollapsed, setPreviewCollapsed] = useState(false);

  const handleAppCommand = useCallback((command: string) => {
    if (command === "refresh") {
      setRefreshSignal((n) => n + 1);
    }
  }, []);

  const handleBuildAction = useCallback(() => {
    setRefreshSignal((n) => n + 1);
  }, []);

  const handleFixPrompt = useCallback((prompt: string) => {
    sendRef.current?.(prompt);
  }, []);

  const activeChat = chats.find((c) => c.id === activeChatId);
  const currentMode: ChatMode = modeOverride ?? activeChat?.mode ?? "agent";

  const handleNewChat = useCallback(async () => {
    const chat = await create("New Chat", currentMode, activeAppId);
    setActiveChatId(chat.id);
    setModeOverride(null);
  }, [create, currentMode, activeAppId]);

  const handleDeleteChat = useCallback(
    async (chatId: string) => {
      await remove(chatId);
      if (activeChatId === chatId) {
        setActiveChatId(chats.find((c) => c.id !== chatId)?.id || null);
        setModeOverride(null);
      }
    },
    [remove, activeChatId, chats],
  );

  const handleSelectChat = useCallback((chatId: string) => {
    setActiveChatId(chatId);
    setModeOverride(null);
  }, []);

  const handleModeChange = useCallback(
    async (mode: ChatMode) => {
      setModeOverride(mode);
      if (activeChatId) {
        try {
          await updateMode(activeChatId, mode);
          setModeOverride(null);
        } catch (err) {
          console.error("Failed to update chat mode:", err);
        }
      }
    },
    [activeChatId, updateMode],
  );

  const handleCreateApp = useCallback(
    async (name: string, template?: string) => {
      const app = await createApp(name, template);
      setActiveAppId(app.id);
      return app;
    },
    [createApp],
  );

  const handleDeleteApp = useCallback(
    async (appId: string) => {
      await removeApp(appId);
      if (activeAppId === appId) setActiveAppId(null);
    },
    [removeApp, activeAppId],
  );

  const handleToggleSidebar = useCallback(() => {
    setSidebarCollapsed((prev) => !prev);
  }, []);

  const toggleChatPanel = useCallback(() => {
    const panel = chatPanelRef.current;
    if (!panel) return;
    if (chatCollapsed) panel.expand();
    else panel.collapse();
  }, [chatCollapsed]);

  const togglePreviewPanel = useCallback(() => {
    const panel = previewPanelRef.current;
    if (!panel) return;
    if (previewCollapsed) panel.expand();
    else panel.collapse();
  }, [previewCollapsed]);

  const handleEditWithAI = useCallback((element: SelectedElement) => {
    setVisualEditContext({
      filePath: element.filePath,
      line: element.line,
      componentName: element.devxName,
    });
  }, []);

  const handleComponentsSelected = useCallback((components: SelectedComponent[]) => {
    setSelectedComponents(components);
  }, []);

  // Keyboard shortcuts
  const shortcutHandlers = useMemo(() => ({
    onNewChat: handleNewChat,
  }), [handleNewChat]);
  useKeyboardShortcuts(shortcutHandlers);

  return (
    <div className="h-screen flex flex-col">
      {/* Header */}
      <header className="flex items-center justify-between border-b px-4 h-12 shrink-0">
        <div className="flex items-center gap-3">
          <h1 className="text-sm font-semibold">DevX</h1>
          <AppSelector
            apps={apps}
            activeAppId={activeAppId}
            onSelectApp={setActiveAppId}
            onCreateApp={handleCreateApp}
            onDeleteApp={handleDeleteApp}
          />
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={toggleChatPanel}
            title={chatCollapsed ? "Show chat" : "Hide chat"}
          >
            {chatCollapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={togglePreviewPanel}
            title={previewCollapsed ? "Show preview" : "Hide preview"}
          >
            {previewCollapsed ? <PanelRightOpen className="h-4 w-4" /> : <PanelRightClose className="h-4 w-4" />}
          </Button>
          <ThemeToggle />
          <Link to="/settings">
            <Button variant="ghost" size="icon" className="h-8 w-8">
              <Settings className="h-4 w-4" />
            </Button>
          </Link>
        </div>
      </header>

      {/* 3-panel layout */}
      <PanelGroup direction="horizontal" className="flex-1">
        <Panel
          defaultSize={sidebarCollapsed ? 4 : 20}
          minSize={sidebarCollapsed ? 3 : 15}
          maxSize={sidebarCollapsed ? 5 : 35}
        >
          <ChatSidebar
            chats={chats}
            activeChatId={activeChatId}
            onSelectChat={handleSelectChat}
            onNewChat={handleNewChat}
            onDeleteChat={handleDeleteChat}
            collapsed={sidebarCollapsed}
            onToggleCollapse={handleToggleSidebar}
          />
        </Panel>

        <PanelResizeHandle
          onDragging={setIsResizing}
          className="w-1 bg-border hover:bg-primary/20 transition-colors cursor-col-resize"
        />

        <Panel
          ref={chatPanelRef}
          defaultSize={50}
          minSize={30}
          collapsible
          onCollapse={() => setChatCollapsed(true)}
          onExpand={() => setChatCollapsed(false)}
          className={cn(!isResizing && "transition-all duration-100 ease-in-out")}
        >
          <ChatPanel
            chatId={activeChatId}
            mode={currentMode}
            onModeChange={handleModeChange}
            onPlanContentChange={setPlanContent}
            visualEditContext={visualEditContext}
            onClearVisualEditContext={() => setVisualEditContext(null)}
            selectedComponents={selectedComponents}
            onRemoveSelectedComponent={(devxId) => setSelectedComponents((prev) => prev.filter((c) => c.devxId !== devxId))}
            onClearSelectedComponents={() => setSelectedComponents([])}
            onAppCommand={handleAppCommand}
            onBuildAction={handleBuildAction}
            sendRef={sendRef}
          />
        </Panel>

        <PanelResizeHandle
          onDragging={setIsResizing}
          className="w-1 bg-border hover:bg-primary/20 transition-colors cursor-col-resize"
        />

        <Panel
          ref={previewPanelRef}
          defaultSize={30}
          minSize={20}
          collapsible
          onCollapse={() => setPreviewCollapsed(true)}
          onExpand={() => setPreviewCollapsed(false)}
          className={cn(!isResizing && "transition-all duration-100 ease-in-out")}
        >
          <PreviewPanel appId={activeAppId} planContent={planContent} chatMode={currentMode} onEditWithAI={handleEditWithAI} onComponentsSelected={handleComponentsSelected} refreshSignal={refreshSignal} onFixPrompt={handleFixPrompt} />
        </Panel>
      </PanelGroup>
    </div>
  );
}

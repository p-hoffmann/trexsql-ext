import { useEffect } from "react";
import { MessagesList } from "./chat/MessagesList";
import { ChatInput } from "./chat/ChatInput";
import { PlanQuestionnaire } from "./chat/PlanQuestionnaire";
import { useMessages } from "@/hooks/useMessages";
import type { ChatMode } from "@/lib/types";
import type { VisualEditContext, SelectedComponent } from "@/lib/visual-editing-types";

interface ChatPanelProps {
  chatId: string | null;
  mode: ChatMode;
  onModeChange: (mode: ChatMode) => void;
  onPlanContentChange?: (content: string | null) => void;
  visualEditContext?: VisualEditContext | null;
  onClearVisualEditContext?: () => void;
  selectedComponents?: SelectedComponent[];
  onRemoveSelectedComponent?: (devxId: string) => void;
  onClearSelectedComponents?: () => void;
  onAppCommand?: (command: string) => void;
  onBuildAction?: (action: import("@/lib/types").BuildAction) => void;
  sendRef?: React.MutableRefObject<((msg: string) => void) | null>;
}

export function ChatPanel({ chatId, mode, onModeChange, onPlanContentChange, visualEditContext, onClearVisualEditContext, selectedComponents, onRemoveSelectedComponent, onClearSelectedComponents, onAppCommand, onBuildAction, sendRef }: ChatPanelProps) {
  const {
    messages,
    streaming,
    streamingContent,
    error: _error,
    todos,
    toolCalls,
    completedToolCalls,
    completedBuildTags,
    buildActions,
    consentRequest,
    consentError,
    questionnaire,
    planContent,
    tokenUsage,
    send,
    cancel,
    resolveConsent,
    answerQuestionnaire,
  } = useMessages(chatId, { onAppCommand, onBuildAction, onModeChange: (m) => onModeChange(m as ChatMode) });

  // Propagate plan content to parent for preview panel
  useEffect(() => {
    onPlanContentChange?.(planContent ?? null);
  }, [planContent, onPlanContentChange]);

  // Expose send function to parent for fix prompts
  useEffect(() => {
    if (sendRef) sendRef.current = send;
    return () => { if (sendRef) sendRef.current = null; };
  }, [send, sendRef]);

  return (
    <div className="flex h-full flex-col">
      <MessagesList
        messages={messages}
        streaming={streaming}
        streamingContent={streamingContent}
        toolCalls={toolCalls}
        completedToolCalls={completedToolCalls}
        completedBuildTags={completedBuildTags}
        buildActions={buildActions}
        onAction={(msg) => send(msg)}
      />
      {questionnaire && (
        <PlanQuestionnaire
          questionnaire={questionnaire}
          onAnswer={answerQuestionnaire}
          onDismiss={() => answerQuestionnaire({})}
        />
      )}
      <ChatInput
        onSend={(message) => {
          send(message, {
            visualEdit: visualEditContext || undefined,
            selectedComponents: selectedComponents && selectedComponents.length > 0 ? selectedComponents : undefined,
          });
          onClearVisualEditContext?.();
          onClearSelectedComponents?.();
        }}
        onCancel={cancel}
        streaming={streaming}
        disabled={!chatId}
        mode={mode}
        onModeChange={onModeChange}
        todos={todos}
        consentRequest={consentRequest}
        consentError={consentError}
        onConsentDecision={resolveConsent}
        messages={messages}
        tokenUsage={tokenUsage}
        visualEditContext={visualEditContext}
        onClearVisualEditContext={onClearVisualEditContext}
        selectedComponents={selectedComponents}
        onRemoveSelectedComponent={onRemoveSelectedComponent}
      />
    </div>
  );
}

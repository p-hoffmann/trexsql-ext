import { Bot, ShieldCheck, Check, Ban } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ConsentRequest } from "@/lib/types";

interface AgentConsentBannerProps {
  consent: ConsentRequest;
  onDecision: (decision: "allow" | "deny" | "always") => void;
}

export function AgentConsentBanner({ consent, onDecision }: AgentConsentBannerProps) {
  return (
    <div className="mx-3 mb-2 rounded-lg border bg-muted/50 p-3">
      <div className="flex items-center gap-2 mb-2">
        <Bot className="h-4 w-4 text-muted-foreground" />
        <span className="text-sm font-medium">
          Agent wants to use: <strong>{consent.toolName}</strong>
        </span>
      </div>
      {consent.inputPreview && (
        <pre className="text-xs text-muted-foreground bg-background rounded p-2 mb-2 overflow-x-auto max-h-24 overflow-y-auto">
          {consent.inputPreview}
        </pre>
      )}
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          variant="outline"
          className="h-7 text-xs"
          onClick={() => onDecision("always")}
        >
          <ShieldCheck className="h-3 w-3 mr-1" />
          Always allow
        </Button>
        <Button
          size="sm"
          variant="outline"
          className="h-7 text-xs"
          onClick={() => onDecision("allow")}
        >
          <Check className="h-3 w-3 mr-1" />
          Allow once
        </Button>
        <Button
          size="sm"
          variant="outline"
          className="h-7 text-xs text-destructive"
          onClick={() => onDecision("deny")}
        >
          <Ban className="h-3 w-3 mr-1" />
          Decline
        </Button>
      </div>
    </div>
  );
}

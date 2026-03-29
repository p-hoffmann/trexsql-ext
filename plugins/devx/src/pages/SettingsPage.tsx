import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  Settings,
  Bot,
  Cpu,
  Plug,
  Github,
  Check,
  X,
  Trash2,
  Plus,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { useSettings } from "@/hooks/useSettings";
import { useGitHub } from "@/hooks/useGitHub";
import { useMcpServers } from "@/hooks/useMcpServers";
import { useTheme } from "@/hooks/useTheme";
import {
  PROVIDERS,
  CHAT_MODES,
  type Provider,
  type ChatMode,
  type PanelContent,
} from "@/lib/types";
import { useLayoutMode } from "@/hooks/useLayoutMode";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { getLanguage, setLanguage, getAvailableLanguages } from "@/lib/i18n";

type Section = "general" | "ai" | "agent" | "integrations";

const SECTIONS: { id: Section; label: string; icon: React.ElementType }[] = [
  { id: "general", label: "General", icon: Settings },
  { id: "ai", label: "AI", icon: Cpu },
  { id: "agent", label: "Agent", icon: Bot },
  { id: "integrations", label: "Integrations", icon: Plug },
];

export default function SettingsPage() {
  const navigate = useNavigate();
  const { settings, save } = useSettings();
  const github = useGitHub();
  const mcp = useMcpServers();
  const { theme, setTheme } = useTheme();
  const { panelAssignment, setPanelAssignment } = useLayoutMode();

  const [activeSection, setActiveSection] = useState<Section>("general");
  const [saving, setSaving] = useState(false);

  // AI fields
  const [provider, setProvider] = useState<Provider>("anthropic");
  const [model, setModel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [baseUrl, setBaseUrl] = useState("");
  const [aiRules, setAiRules] = useState("");
  // AWS Bedrock fields
  const [awsAccessKeyId, setAwsAccessKeyId] = useState("");
  const [awsSecretAccessKey, setAwsSecretAccessKey] = useState("");
  const [awsRegion, setAwsRegion] = useState("us-east-1");
  const [awsBearerToken, setAwsBearerToken] = useState("");
  const [awsAuthMode, setAwsAuthMode] = useState<"bearer" | "iam">("bearer");

  // Agent fields
  const [autoApprove, setAutoApprove] = useState(false);
  const [maxSteps, setMaxSteps] = useState(25);
  const [maxToolSteps, setMaxToolSteps] = useState(10);
  const [autoFixProblems, setAutoFixProblems] = useState(false);

  // General fields
  const [defaultChatMode, setDefaultChatMode] = useState<ChatMode>("agent");
  const [language, setLang] = useState(getLanguage());

  // MCP fields
  const [mcpName, setMcpName] = useState("");
  const [mcpTransport, setMcpTransport] = useState<"stdio" | "http">("stdio");
  const [mcpCommand, setMcpCommand] = useState("");
  const [mcpUrl, setMcpUrl] = useState("");

  // Sync from loaded settings
  useEffect(() => {
    if (settings) {
      setProvider((settings.provider as Provider) || "anthropic");
      setModel(settings.model || "");
      setBaseUrl(settings.base_url || "");
      setAiRules(settings.ai_rules || "");
      setAutoApprove(settings.auto_approve ?? false);
      setMaxSteps(settings.max_steps ?? 25);
      setMaxToolSteps(settings.max_tool_steps ?? 10);
      setAutoFixProblems(settings.auto_fix_problems ?? false);

      // Unpack Bedrock credentials from api_key JSON
      if (settings.provider === "bedrock" && settings.api_key) {
        try {
          const creds = JSON.parse(settings.api_key);
          if (creds.bearerToken) {
            setAwsAuthMode("bearer");
            setAwsBearerToken(creds.bearerToken);
            setAwsAccessKeyId("");
            setAwsSecretAccessKey("");
          } else {
            setAwsAuthMode("iam");
            setAwsAccessKeyId(creds.accessKeyId || "");
            setAwsSecretAccessKey(creds.secretAccessKey || "");
            setAwsBearerToken("");
          }
          setAwsRegion(settings.base_url || "us-east-1");
          setApiKey("");
        } catch {
          setApiKey(settings.api_key || "");
        }
      } else {
        setApiKey(settings.api_key || "");
      }
    }
  }, [settings]);

  const providerConfig = PROVIDERS.find((p) => p.id === provider);

  const handleSave = async () => {
    setSaving(true);
    try {
      // Pack Bedrock credentials into api_key/base_url columns
      const effectiveApiKey = provider === "bedrock"
        ? JSON.stringify(
            awsAuthMode === "bearer"
              ? { bearerToken: awsBearerToken }
              : { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
          )
        : apiKey;
      const effectiveBaseUrl = provider === "bedrock" ? awsRegion : baseUrl;

      await save({
        provider,
        model,
        api_key: effectiveApiKey,
        base_url: effectiveBaseUrl,
        ai_rules: aiRules || undefined,
        auto_approve: autoApprove,
        max_steps: maxSteps,
        max_tool_steps: maxToolSteps,
        auto_fix_problems: autoFixProblems,
      });
      toast.success("Settings saved");
    } catch (err) {
      console.error("Failed to save settings:", err);
      toast.error("Failed to save settings");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="h-screen flex flex-col bg-background">
      {/* Header */}
      <header className="flex items-center gap-3 border-b px-4 h-12 shrink-0">
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8"
          onClick={() => navigate("/")}
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <h1 className="text-sm font-semibold">Settings</h1>
        <div className="flex-1" />
        <Button size="sm" onClick={handleSave} disabled={saving}>
          {saving ? "Saving..." : "Save"}
        </Button>
      </header>

      {/* Body: sidebar + content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left sidebar nav */}
        <nav className="w-48 border-r p-2 space-y-1 shrink-0">
          {SECTIONS.map((s) => {
            const Icon = s.icon;
            return (
              <button
                key={s.id}
                onClick={() => setActiveSection(s.id)}
                className={cn(
                  "flex items-center gap-2 w-full rounded-md px-3 py-2 text-sm transition-colors",
                  activeSection === s.id
                    ? "bg-accent text-accent-foreground font-medium"
                    : "text-muted-foreground hover:bg-accent/50 hover:text-foreground",
                )}
              >
                <Icon className="h-4 w-4" />
                {s.label}
              </button>
            );
          })}
        </nav>

        {/* Right content */}
        <div className="flex-1 overflow-y-auto p-6 max-w-2xl">
          {activeSection === "general" && (
            <div className="space-y-6">
              <div>
                <h2 className="text-lg font-semibold mb-1">General</h2>
                <p className="text-sm text-muted-foreground">
                  Appearance and default behavior.
                </p>
              </div>
              <Separator />

              {/* Theme */}
              <div className="space-y-2">
                <Label>Theme</Label>
                <select
                  value={theme}
                  onChange={(e) =>
                    setTheme(e.target.value as "light" | "dark" | "system")
                  }
                  className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                >
                  <option value="system">System</option>
                  <option value="light">Light</option>
                  <option value="dark">Dark</option>
                </select>
              </div>

              {/* Language */}
              <div className="space-y-2">
                <Label>Language</Label>
                <select
                  value={language}
                  onChange={(e) => {
                    const lang = e.target.value;
                    setLang(lang);
                    setLanguage(lang);
                  }}
                  className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                >
                  {getAvailableLanguages().map((l) => (
                    <option key={l.id} value={l.id}>
                      {l.name}
                    </option>
                  ))}
                </select>
                <p className="text-xs text-muted-foreground">
                  Changes apply after reload.
                </p>
              </div>

              {/* Default chat mode */}
              <div className="space-y-2">
                <Label>Default Chat Mode</Label>
                <select
                  value={defaultChatMode}
                  onChange={(e) =>
                    setDefaultChatMode(e.target.value as ChatMode)
                  }
                  className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                >
                  {CHAT_MODES.map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.label} — {m.description}
                    </option>
                  ))}
                </select>
              </div>

              {/* Panel Layout */}
              <div className="space-y-2">
                <Label>Panel Layout</Label>
                <p className="text-xs text-muted-foreground">
                  Choose what content appears in each panel position.
                </p>
                <div className="flex gap-4">
                  <div className="space-y-1 flex-1">
                    <Label className="text-xs">Left Panel</Label>
                    <select
                      value={panelAssignment.left}
                      onChange={(e) => {
                        const val = e.target.value as PanelContent;
                        setPanelAssignment({
                          left: val,
                          right: val === "chat" ? "preview" : "chat",
                        });
                      }}
                      className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    >
                      <option value="chat">Chat</option>
                      <option value="preview">Preview / Code</option>
                    </select>
                  </div>
                  <div className="space-y-1 flex-1">
                    <Label className="text-xs">Right Panel</Label>
                    <select
                      value={panelAssignment.right}
                      onChange={(e) => {
                        const val = e.target.value as PanelContent;
                        setPanelAssignment({
                          right: val,
                          left: val === "chat" ? "preview" : "chat",
                        });
                      }}
                      className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    >
                      <option value="chat">Chat</option>
                      <option value="preview">Preview / Code</option>
                    </select>
                  </div>
                </div>
              </div>
            </div>
          )}

          {activeSection === "ai" && (
            <div className="space-y-6">
              <div>
                <h2 className="text-lg font-semibold mb-1">AI Provider</h2>
                <p className="text-sm text-muted-foreground">
                  Configure your AI provider, model, and API key.
                </p>
              </div>
              <Separator />

              {/* Provider */}
              <div className="space-y-2">
                <Label>Provider</Label>
                <select
                  value={provider}
                  onChange={(e) => {
                    const p = e.target.value as Provider;
                    setProvider(p);
                    const pc = PROVIDERS.find((x) => x.id === p);
                    if (pc && pc.models.length > 0) {
                      setModel(pc.models[0]);
                    } else {
                      setModel("");
                    }
                  }}
                  className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                >
                  {PROVIDERS.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </select>
              </div>

              {/* Model */}
              <div className="space-y-2">
                <Label>Model</Label>
                {providerConfig && providerConfig.models.length > 0 ? (
                  <select
                    value={model}
                    onChange={(e) => setModel(e.target.value)}
                    className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                  >
                    {providerConfig.models.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                ) : (
                  <Input
                    value={model}
                    onChange={(e) => setModel(e.target.value)}
                    placeholder="Model name"
                  />
                )}
              </div>

              {/* API Key */}
              {providerConfig?.requiresApiKey && (
                <div className="space-y-2">
                  <Label>API Key</Label>
                  <Input
                    type="password"
                    value={apiKey}
                    onChange={(e) => setApiKey(e.target.value)}
                    placeholder="sk-..."
                  />
                </div>
              )}

              {/* Base URL */}
              {providerConfig?.requiresBaseUrl && (
                <div className="space-y-2">
                  <Label>Base URL</Label>
                  <Input
                    value={baseUrl}
                    onChange={(e) => setBaseUrl(e.target.value)}
                    placeholder="https://api.example.com/v1"
                  />
                </div>
              )}

              {/* AWS Bedrock credentials */}
              {provider === "bedrock" && (
                <>
                  <div className="space-y-2">
                    <Label>Authentication</Label>
                    <select
                      value={awsAuthMode}
                      onChange={(e) => setAwsAuthMode(e.target.value as "bearer" | "iam")}
                      className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    >
                      <option value="bearer">Bearer Token</option>
                      <option value="iam">IAM Access Keys</option>
                    </select>
                  </div>

                  {awsAuthMode === "bearer" ? (
                    <div className="space-y-2">
                      <Label>Bearer Token</Label>
                      <Input
                        type="password"
                        value={awsBearerToken}
                        onChange={(e) => setAwsBearerToken(e.target.value)}
                        placeholder="AWS bearer token"
                      />
                      <p className="text-xs text-muted-foreground">
                        Or set the AWS_BEARER_TOKEN_BEDROCK environment variable
                      </p>
                    </div>
                  ) : (
                    <>
                      <div className="space-y-2">
                        <Label>AWS Access Key ID</Label>
                        <Input
                          type="password"
                          value={awsAccessKeyId}
                          onChange={(e) => setAwsAccessKeyId(e.target.value)}
                          placeholder="AKIA..."
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>AWS Secret Access Key</Label>
                        <Input
                          type="password"
                          value={awsSecretAccessKey}
                          onChange={(e) => setAwsSecretAccessKey(e.target.value)}
                          placeholder="wJal..."
                        />
                      </div>
                    </>
                  )}

                  <div className="space-y-2">
                    <Label>AWS Region</Label>
                    <select
                      value={awsRegion}
                      onChange={(e) => setAwsRegion(e.target.value)}
                      className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    >
                      <option value="us-east-1">US East (N. Virginia) — us-east-1</option>
                      <option value="us-east-2">US East (Ohio) — us-east-2</option>
                      <option value="us-west-2">US West (Oregon) — us-west-2</option>
                      <option value="eu-west-1">Europe (Ireland) — eu-west-1</option>
                      <option value="eu-west-2">Europe (London) — eu-west-2</option>
                      <option value="eu-west-3">Europe (Paris) — eu-west-3</option>
                      <option value="eu-central-1">Europe (Frankfurt) — eu-central-1</option>
                      <option value="ap-southeast-1">Asia Pacific (Singapore) — ap-southeast-1</option>
                      <option value="ap-southeast-2">Asia Pacific (Sydney) — ap-southeast-2</option>
                      <option value="ap-northeast-1">Asia Pacific (Tokyo) — ap-northeast-1</option>
                      <option value="ap-south-1">Asia Pacific (Mumbai) — ap-south-1</option>
                      <option value="ca-central-1">Canada (Central) — ca-central-1</option>
                      <option value="sa-east-1">South America (São Paulo) — sa-east-1</option>
                    </select>
                  </div>
                </>
              )}

              {/* Custom AI Rules */}
              <div className="space-y-2">
                <Label>Custom AI Rules</Label>
                <textarea
                  value={aiRules}
                  onChange={(e) => setAiRules(e.target.value)}
                  placeholder="Override default AI rules (tech stack, coding style, etc.)"
                  rows={4}
                  className="flex w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring resize-y"
                />
                <p className="text-xs text-muted-foreground">
                  Leave empty to use defaults (React + TypeScript + Tailwind +
                  shadcn/ui)
                </p>
              </div>
            </div>
          )}

          {activeSection === "agent" && (
            <div className="space-y-6">
              <div>
                <h2 className="text-lg font-semibold mb-1">Agent</h2>
                <p className="text-sm text-muted-foreground">
                  Configure autonomous agent behavior and limits.
                </p>
              </div>
              <Separator />

              {/* Auto-approve */}
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label htmlFor="auto-approve">Auto-approve tool calls</Label>
                  <p className="text-xs text-muted-foreground">
                    Skip consent prompts and automatically approve all tool
                    calls.
                  </p>
                </div>
                <Switch
                  id="auto-approve"
                  checked={autoApprove}
                  onCheckedChange={setAutoApprove}
                />
              </div>

              <Separator />

              {/* Max chat turns */}
              <div className="space-y-2">
                <Label htmlFor="max-steps">Max chat turns</Label>
                <p className="text-xs text-muted-foreground">
                  Maximum number of agent loop steps per message (default: 25).
                </p>
                <Input
                  id="max-steps"
                  type="number"
                  min={1}
                  max={100}
                  value={maxSteps}
                  onChange={(e) =>
                    setMaxSteps(parseInt(e.target.value) || 25)
                  }
                  className="w-24"
                />
              </div>

              {/* Max tool call steps */}
              <div className="space-y-2">
                <Label htmlFor="max-tool-steps">Max tool call steps</Label>
                <p className="text-xs text-muted-foreground">
                  Maximum number of consecutive tool calls before pausing
                  (default: 10).
                </p>
                <Input
                  id="max-tool-steps"
                  type="number"
                  min={1}
                  max={50}
                  value={maxToolSteps}
                  onChange={(e) =>
                    setMaxToolSteps(parseInt(e.target.value) || 10)
                  }
                  className="w-24"
                />
              </div>

              <Separator />

              {/* Auto-fix problems */}
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <Label htmlFor="auto-fix">Auto-fix problems</Label>
                  <p className="text-xs text-muted-foreground">
                    Automatically attempt to fix type errors and lint issues
                    after code changes.
                  </p>
                </div>
                <Switch
                  id="auto-fix"
                  checked={autoFixProblems}
                  onCheckedChange={setAutoFixProblems}
                />
              </div>
            </div>
          )}

          {activeSection === "integrations" && (
            <div className="space-y-6">
              <div>
                <h2 className="text-lg font-semibold mb-1">Integrations</h2>
                <p className="text-sm text-muted-foreground">
                  Connect external services and MCP servers.
                </p>
              </div>
              <Separator />

              {/* GitHub */}
              <div className="space-y-3">
                <div className="flex items-center gap-2">
                  <Github className="h-4 w-4" />
                  <Label className="text-base">GitHub</Label>
                </div>
                {github.status.connected ? (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground flex items-center gap-1.5">
                      <Check className="h-3.5 w-3.5 text-green-500" />
                      Connected as {github.status.username}
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 text-xs"
                      onClick={github.disconnect}
                    >
                      Disconnect
                    </Button>
                  </div>
                ) : github.deviceCode ? (
                  <div className="space-y-2 text-sm">
                    <p>Enter this code at GitHub:</p>
                    <div className="flex items-center gap-2">
                      <code className="px-3 py-1.5 bg-muted rounded font-mono text-lg tracking-wider">
                        {github.deviceCode.user_code}
                      </code>
                      <a
                        href={github.deviceCode.verification_uri}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-primary underline"
                      >
                        Open GitHub
                      </a>
                    </div>
                    {github.polling && (
                      <p className="text-xs text-muted-foreground">
                        Waiting for authorization...
                      </p>
                    )}
                  </div>
                ) : (
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-1.5"
                    onClick={github.startDeviceFlow}
                  >
                    <Github className="h-3.5 w-3.5" />
                    Connect GitHub
                  </Button>
                )}
              </div>

              <Separator />

              {/* MCP Servers */}
              <div className="space-y-3">
                <div className="flex items-center gap-2">
                  <Plug className="h-4 w-4" />
                  <Label className="text-base">MCP Servers</Label>
                </div>

                {mcp.servers.map((s) => (
                  <div
                    key={s.id}
                    className="flex items-center justify-between text-sm border rounded px-3 py-2"
                  >
                    <div className="flex items-center gap-2">
                      <div
                        className={`h-2 w-2 rounded-full ${s.enabled ? "bg-green-500" : "bg-gray-400"}`}
                      />
                      <span className="text-xs">{s.name}</span>
                      <span className="text-[10px] text-muted-foreground">
                        ({s.transport})
                      </span>
                    </div>
                    <div className="flex items-center gap-1">
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6"
                        onClick={() => mcp.toggle(s.id, !s.enabled)}
                      >
                        {s.enabled ? (
                          <Check className="h-3 w-3" />
                        ) : (
                          <X className="h-3 w-3" />
                        )}
                      </Button>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 hover:text-destructive"
                        onClick={() => mcp.remove(s.id)}
                      >
                        <Trash2 className="h-3 w-3" />
                      </Button>
                    </div>
                  </div>
                ))}

                <div className="flex items-center gap-2">
                  <Input
                    value={mcpName}
                    onChange={(e) => setMcpName(e.target.value)}
                    placeholder="Server name"
                    className="h-7 text-xs flex-1"
                  />
                  <select
                    value={mcpTransport}
                    onChange={(e) =>
                      setMcpTransport(e.target.value as "stdio" | "http")
                    }
                    className="h-7 rounded border text-xs px-1"
                  >
                    <option value="stdio">stdio</option>
                    <option value="http">http</option>
                  </select>
                </div>
                {mcpTransport === "stdio" ? (
                  <Input
                    value={mcpCommand}
                    onChange={(e) => setMcpCommand(e.target.value)}
                    placeholder="Command (e.g. npx -y @mcp/server)"
                    className="h-7 text-xs"
                  />
                ) : (
                  <Input
                    value={mcpUrl}
                    onChange={(e) => setMcpUrl(e.target.value)}
                    placeholder="Server URL (e.g. http://localhost:3100/mcp)"
                    className="h-7 text-xs"
                  />
                )}
                <Button
                  variant="outline"
                  size="sm"
                  className="gap-1 text-xs w-full"
                  disabled={!mcpName.trim()}
                  onClick={async () => {
                    await mcp.create({
                      name: mcpName.trim(),
                      transport: mcpTransport,
                      command:
                        mcpTransport === "stdio" ? mcpCommand : undefined,
                      url: mcpTransport === "http" ? mcpUrl : undefined,
                    });
                    setMcpName("");
                    setMcpCommand("");
                    setMcpUrl("");
                  }}
                >
                  <Plus className="h-3 w-3" />
                  Add Server
                </Button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

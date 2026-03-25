import { useState, useEffect } from "react";
import { Settings, Github, Plug, Plus, Trash2, Check, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { PROVIDERS, type DevxSettings, type Provider } from "@/lib/types";
import { useGitHub } from "@/hooks/useGitHub";
import { useMcpServers } from "@/hooks/useMcpServers";

interface SettingsDialogProps {
  settings: DevxSettings | null;
  onSave: (settings: Partial<DevxSettings>) => Promise<DevxSettings>;
}

export function SettingsDialog({ settings, onSave }: SettingsDialogProps) {
  const [open, setOpen] = useState(false);
  const [provider, setProvider] = useState<Provider>("anthropic");
  const [model, setModel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [baseUrl, setBaseUrl] = useState("");
  const [aiRules, setAiRules] = useState("");
  const [saving, setSaving] = useState(false);
  // AWS Bedrock fields
  const [awsAccessKeyId, setAwsAccessKeyId] = useState("");
  const [awsSecretAccessKey, setAwsSecretAccessKey] = useState("");
  const [awsRegion, setAwsRegion] = useState("us-east-1");
  const [awsBearerToken, setAwsBearerToken] = useState("");
  const [awsAuthMode, setAwsAuthMode] = useState<"bearer" | "iam">("bearer");
  const github = useGitHub();
  const mcp = useMcpServers();
  const [mcpName, setMcpName] = useState("");
  const [mcpTransport, setMcpTransport] = useState<"stdio" | "http">("stdio");
  const [mcpCommand, setMcpCommand] = useState("");
  const [mcpUrl, setMcpUrl] = useState("");

  useEffect(() => {
    if (settings) {
      setProvider((settings.provider as Provider) || "anthropic");
      setModel(settings.model || "");
      setBaseUrl(settings.base_url || "");
      setAiRules(settings.ai_rules || "");

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
      const effectiveApiKey = provider === "bedrock"
        ? JSON.stringify(
            awsAuthMode === "bearer"
              ? { bearerToken: awsBearerToken }
              : { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
          )
        : apiKey;
      const effectiveBaseUrl = provider === "bedrock" ? awsRegion : baseUrl;
      await onSave({ provider, model, api_key: effectiveApiKey, base_url: effectiveBaseUrl, ai_rules: aiRules || undefined });
      setOpen(false);
    } catch (err) {
      console.error("Failed to save settings:", err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="ghost" size="icon" className="h-8 w-8">
          <Settings className="h-4 w-4" />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Settings</DialogTitle>
          <DialogDescription>
            Configure your AI provider and model.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4 py-2">
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
              Leave empty to use defaults (React + TypeScript + Tailwind + shadcn/ui)
            </p>
          </div>
        </div>

        <Separator />

        {/* GitHub Integration */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <Github className="h-4 w-4" />
            <Label>GitHub</Label>
          </div>
          {github.status.connected ? (
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground flex items-center gap-1.5">
                <Check className="h-3.5 w-3.5 text-green-500" />
                Connected as {github.status.username}
              </span>
              <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={github.disconnect}>
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
              {github.polling && <p className="text-xs text-muted-foreground">Waiting for authorization...</p>}
            </div>
          ) : (
            <Button variant="outline" size="sm" className="gap-1.5" onClick={github.startDeviceFlow}>
              <Github className="h-3.5 w-3.5" />
              Connect GitHub
            </Button>
          )}
        </div>

        <Separator />

        {/* MCP Servers */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <Plug className="h-4 w-4" />
            <Label>MCP Servers</Label>
          </div>
          {mcp.servers.map((s) => (
            <div key={s.id} className="flex items-center justify-between text-sm border rounded px-2 py-1.5">
              <div className="flex items-center gap-2">
                <div className={`h-2 w-2 rounded-full ${s.enabled ? "bg-green-500" : "bg-gray-400"}`} />
                <span className="text-xs">{s.name}</span>
                <span className="text-[10px] text-muted-foreground">({s.transport})</span>
              </div>
              <div className="flex items-center gap-1">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6"
                  onClick={() => mcp.toggle(s.id, !s.enabled)}
                >
                  {s.enabled ? <Check className="h-3 w-3" /> : <X className="h-3 w-3" />}
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
              onChange={(e) => setMcpTransport(e.target.value as "stdio" | "http")}
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
                command: mcpTransport === "stdio" ? mcpCommand : undefined,
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

        <Separator />

        <div className="flex justify-end">
          <Button onClick={handleSave} disabled={saving}>
            {saving ? "Saving..." : "Save AI Settings"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}

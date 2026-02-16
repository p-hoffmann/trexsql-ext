import { useState, useEffect, useCallback } from "react";
import { toast } from "sonner";
import { authClient, useSession } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  MonitorIcon,
  SmartphoneIcon,
  LinkIcon,
  UnlinkIcon,
  ShieldIcon,
  Loader2Icon,
} from "lucide-react";

// --- Types ---

interface SessionInfo {
  id: string;
  token: string;
  ipAddress: string | null;
  userAgent: string | null;
  createdAt: string;
  expiresAt: string;
}

interface AccountInfo {
  id: string;
  providerId: string;
  accountId: string;
  createdAt: string;
}

// Known social providers the system supports.
// Extend this list as new providers are configured in Better Auth.
const KNOWN_PROVIDERS = ["github", "google", "microsoft", "apple"] as const;

// --- Helpers ---

function parseUserAgent(ua: string | null): {
  browser: string;
  device: string;
} {
  if (!ua) return { browser: "Unknown", device: "Unknown" };

  let browser = "Unknown";
  if (ua.includes("Edg/") || ua.includes("Edge/")) browser = "Edge";
  else if (ua.includes("Chrome/")) browser = "Chrome";
  else if (ua.includes("Firefox/")) browser = "Firefox";
  else if (ua.includes("Safari/") && !ua.includes("Chrome/"))
    browser = "Safari";

  let device = "Desktop";
  if (/Mobile|Android|iPhone|iPad/i.test(ua)) device = "Mobile";

  return { browser, device };
}

function providerDisplayName(provider: string): string {
  const names: Record<string, string> = {
    github: "GitHub",
    google: "Google",
    microsoft: "Microsoft",
    apple: "Apple",
    credential: "Email & Password",
  };
  return names[provider] || provider.charAt(0).toUpperCase() + provider.slice(1);
}

// --- Profile Tab ---

function ProfileTab() {
  const { data: session, isPending } = useSession();
  const [name, setName] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (session?.user?.name) {
      setName(session.user.name);
    }
  }, [session]);

  async function handleSave() {
    setSaving(true);
    try {
      const result = await authClient.updateUser({ name });
      if (result.error) {
        toast.error(result.error.message || "Failed to update profile.");
        return;
      }
      toast.success("Profile updated.");
    } catch {
      toast.error("An unexpected error occurred.");
    } finally {
      setSaving(false);
    }
  }

  if (isPending) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!session) return null;

  const initials =
    session.user.name
      ?.split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase() || "?";

  return (
    <Card>
      <CardHeader>
        <CardTitle>Profile Information</CardTitle>
        <CardDescription>Update your display name and avatar</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-6">
          <div className="flex items-center gap-4">
            <Avatar className="h-16 w-16" size="lg">
              <AvatarImage src={session.user.image || undefined} />
              <AvatarFallback className="text-lg">{initials}</AvatarFallback>
            </Avatar>
            <div>
              <p className="font-medium">{session.user.name}</p>
              <p className="text-sm text-muted-foreground">
                {session.user.email}
              </p>
            </div>
          </div>

          <Separator />

          <div className="flex flex-col gap-4 max-w-md">
            <div className="flex flex-col gap-2">
              <Label htmlFor="profile-name">Display Name</Label>
              <Input
                id="profile-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Your name"
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="profile-email">Email</Label>
              <Input
                id="profile-email"
                value={session.user.email}
                disabled
                readOnly
              />
              <p className="text-xs text-muted-foreground">
                Email cannot be changed
              </p>
            </div>

            <Button
              onClick={handleSave}
              disabled={saving || name === session.user.name}
              className="w-fit"
            >
              {saving ? "Saving..." : "Save Changes"}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// --- Security Tab (password change) ---

function SecurityTab() {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleChangePassword(e: React.FormEvent) {
    e.preventDefault();
    setError("");

    if (newPassword !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    setLoading(true);
    try {
      const result = await authClient.changePassword({
        currentPassword,
        newPassword,
        revokeOtherSessions: false,
      });

      if (result.error) {
        setError(result.error.message || "Failed to change password.");
        return;
      }

      toast.success("Password changed successfully.");
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
    } catch {
      setError("An unexpected error occurred.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Change Password</CardTitle>
        <CardDescription>Update your account password</CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleChangePassword} className="flex flex-col gap-4 max-w-md">
          <div className="flex flex-col gap-2">
            <Label htmlFor="current-password">Current Password</Label>
            <Input
              id="current-password"
              type="password"
              value={currentPassword}
              onChange={(e) => setCurrentPassword(e.target.value)}
              required
              autoComplete="current-password"
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="new-password">New Password</Label>
            <Input
              id="new-password"
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              required
              minLength={8}
              autoComplete="new-password"
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="confirm-password">Confirm New Password</Label>
            <Input
              id="confirm-password"
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              required
              minLength={8}
              autoComplete="new-password"
            />
          </div>

          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}

          <Button type="submit" className="w-fit" disabled={loading}>
            {loading ? "Changing..." : "Change Password"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

// --- Linked Accounts Tab (T045 + T047 lockout protection) ---

function LinkedAccountsTab() {
  const [accounts, setAccounts] = useState<AccountInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [unlinking, setUnlinking] = useState<string | null>(null);
  const [linking, setLinking] = useState<string | null>(null);

  const fetchAccounts = useCallback(async () => {
    setLoading(true);
    try {
      const result = await authClient.listAccounts();
      if (result.data) {
        setAccounts(result.data as unknown as AccountInfo[]);
      }
    } catch {
      toast.error("Failed to load linked accounts.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAccounts();
  }, [fetchAccounts]);

  // T047: Check if user has at least one other auth method before allowing unlink.
  // The user must retain at least one linked account (social or credential).
  const canUnlink = accounts.length > 1;

  async function handleUnlink(providerId: string) {
    setUnlinking(providerId);
    try {
      const result = await authClient.unlinkAccount({ providerId });
      if (result.error) {
        toast.error(
          result.error.message || "Failed to unlink account."
        );
        return;
      }
      toast.success(`${providerDisplayName(providerId)} account unlinked.`);
      fetchAccounts();
    } catch {
      toast.error("Failed to unlink account.");
    } finally {
      setUnlinking(null);
    }
  }

  async function handleLink(provider: string) {
    setLinking(provider);
    try {
      await authClient.signIn.social({
        provider: provider as "github" | "google" | "microsoft" | "apple",
        callbackURL: window.location.href,
      });
    } catch {
      toast.error(`Failed to link ${providerDisplayName(provider)} account.`);
      setLinking(null);
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const linkedProviders = accounts.map((a) => a.providerId);
  const unlinkedProviders = KNOWN_PROVIDERS.filter(
    (p) => !linkedProviders.includes(p)
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>Linked Accounts</CardTitle>
        <CardDescription>
          Manage your connected authentication providers
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          {/* Linked accounts */}
          {accounts.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No linked accounts found.
            </p>
          ) : (
            accounts.map((account) => (
              <div
                key={account.id}
                className="flex items-center justify-between rounded-md border p-4"
              >
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                    <LinkIcon className="size-4" />
                  </div>
                  <div>
                    <p className="text-sm font-medium">
                      {providerDisplayName(account.providerId)}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Linked{" "}
                      {account.createdAt
                        ? new Date(account.createdAt).toLocaleDateString()
                        : ""}
                    </p>
                  </div>
                </div>
                {account.providerId !== "credential" && (
                  <div className="relative group">
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={!canUnlink || unlinking === account.providerId}
                      onClick={() => handleUnlink(account.providerId)}
                    >
                      <UnlinkIcon className="size-4 mr-1" />
                      {unlinking === account.providerId
                        ? "Unlinking..."
                        : "Unlink"}
                    </Button>
                    {!canUnlink && (
                      <div className="absolute bottom-full right-0 mb-2 hidden group-hover:block w-56 rounded-md bg-popover border p-2 shadow-md text-xs text-muted-foreground z-50">
                        Cannot unlink your only authentication method. Link
                        another provider or set a password first.
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))
          )}

          {/* Unlinked providers */}
          {unlinkedProviders.length > 0 && (
            <>
              <Separator />
              <div>
                <p className="text-sm font-medium mb-3">
                  Available Providers
                </p>
                <div className="flex flex-wrap gap-2">
                  {unlinkedProviders.map((provider) => (
                    <Button
                      key={provider}
                      variant="outline"
                      size="sm"
                      disabled={linking === provider}
                      onClick={() => handleLink(provider)}
                    >
                      <LinkIcon className="size-4 mr-1" />
                      {linking === provider
                        ? "Linking..."
                        : `Link ${providerDisplayName(provider)}`}
                    </Button>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// --- Sessions Tab (T046) ---

function SessionsTab() {
  const { data: currentSession } = useSession();
  const [sessions, setSessions] = useState<SessionInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [revoking, setRevoking] = useState<string | null>(null);

  const fetchSessions = useCallback(async () => {
    setLoading(true);
    try {
      const result = await authClient.listSessions();
      if (result.data) {
        setSessions(result.data as unknown as SessionInfo[]);
      }
    } catch {
      toast.error("Failed to load sessions.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSessions();
  }, [fetchSessions]);

  async function handleRevoke(sessionToken: string) {
    setRevoking(sessionToken);
    try {
      const result = await authClient.revokeSession({ token: sessionToken });
      if (result.error) {
        toast.error(result.error.message || "Failed to revoke session.");
        return;
      }
      toast.success("Session revoked.");
      fetchSessions();
    } catch {
      toast.error("Failed to revoke session.");
    } finally {
      setRevoking(null);
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const currentToken = currentSession?.session?.token;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Active Sessions</CardTitle>
        <CardDescription>
          Manage your active sessions across devices ({sessions.length} total)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-3">
          {sessions.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No active sessions found.
            </p>
          ) : (
            sessions.map((session) => {
              const { browser, device } = parseUserAgent(session.userAgent);
              const isCurrent = session.token === currentToken;
              const DeviceIcon =
                device === "Mobile" ? SmartphoneIcon : MonitorIcon;

              return (
                <div
                  key={session.id}
                  className="flex items-center justify-between rounded-md border p-4"
                >
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                      <DeviceIcon className="size-4" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <p className="text-sm font-medium">
                          {browser} on {device}
                        </p>
                        {isCurrent && (
                          <Badge variant="default" className="text-xs">
                            <ShieldIcon className="size-3 mr-0.5" />
                            Current
                          </Badge>
                        )}
                      </div>
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        {session.ipAddress && (
                          <span className="font-mono">
                            {session.ipAddress}
                          </span>
                        )}
                        <span>
                          Created{" "}
                          {new Date(session.createdAt).toLocaleDateString()}
                        </span>
                      </div>
                    </div>
                  </div>
                  {!isCurrent && (
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={revoking === session.token}
                      onClick={() => handleRevoke(session.token)}
                    >
                      {revoking === session.token ? "Revoking..." : "Revoke"}
                    </Button>
                  )}
                </div>
              );
            })
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// --- Main Profile Page ---

export function Profile() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold">Account Settings</h2>
        <p className="text-muted-foreground">
          Manage your profile, security, linked accounts, and sessions
        </p>
      </div>

      <Tabs defaultValue="profile">
        <TabsList>
          <TabsTrigger value="profile">Profile</TabsTrigger>
          <TabsTrigger value="security">Security</TabsTrigger>
          <TabsTrigger value="accounts">Linked Accounts</TabsTrigger>
          <TabsTrigger value="sessions">Sessions</TabsTrigger>
        </TabsList>

        <TabsContent value="profile" className="mt-4">
          <ProfileTab />
        </TabsContent>

        <TabsContent value="security" className="mt-4">
          <SecurityTab />
        </TabsContent>

        <TabsContent value="accounts" className="mt-4">
          <LinkedAccountsTab />
        </TabsContent>

        <TabsContent value="sessions" className="mt-4">
          <SessionsTab />
        </TabsContent>
      </Tabs>
    </div>
  );
}

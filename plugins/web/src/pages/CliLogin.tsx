import { useState } from "react";
import { useSearchParams, Navigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { BASE_PATH } from "@/lib/config";
import { useSession, authClient } from "@/lib/auth-client";
import { toast } from "sonner";

export function CliLogin() {
  const [searchParams] = useSearchParams();
  const { data: session, isPending } = useSession();
  const [submitting, setSubmitting] = useState(false);
  const [deviceCode, setDeviceCode] = useState<string | null>(null);

  const sessionId = searchParams.get("session_id") || "";
  const tokenName = searchParams.get("token_name") || "CLI";
  const publicKey = searchParams.get("public_key") || "";

  if (isPending) return null;

  // Not logged in — redirect to login with a redirect back here
  if (!session) {
    const redirectPath = `/cli/login?${searchParams.toString()}`;
    return <Navigate to={`/login?redirect=${encodeURIComponent(redirectPath)}`} replace />;
  }

  // Must be admin
  if (session.user.role !== "admin") {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-4">
        <Card className="w-full max-w-md">
          <CardHeader className="text-center">
            <CardTitle className="text-2xl">Access Denied</CardTitle>
            <CardDescription>
              Admin access is required to authorize CLI login.
            </CardDescription>
          </CardHeader>
        </Card>
      </div>
    );
  }

  // Missing required params
  if (!sessionId || !publicKey) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-4">
        <Card className="w-full max-w-md">
          <CardHeader className="text-center">
            <CardTitle className="text-2xl">Invalid Request</CardTitle>
            <CardDescription>
              Missing required parameters. Please try logging in from the CLI again.
            </CardDescription>
          </CardHeader>
        </Card>
      </div>
    );
  }

  async function handleApprove() {
    setSubmitting(true);
    try {
      const token = authClient.getAccessToken();
      const res = await fetch(`${BASE_PATH}/api/cli/sessions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          session_id: sessionId,
          public_key: publicKey,
          token_name: tokenName,
        }),
      });

      if (res.ok) {
        const data = await res.json();
        setDeviceCode(data.device_code);
        toast.success("CLI login approved");
      } else {
        const data = await res.json().catch(() => ({}));
        toast.error(data?.error || "Failed to approve CLI login");
      }
    } catch {
      toast.error("An error occurred");
    } finally {
      setSubmitting(false);
    }
  }

  // After approval — show the verification code
  if (deviceCode) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-4">
        <Card className="w-full max-w-md">
          <CardHeader className="text-center">
            <CardTitle className="text-2xl">Login Approved</CardTitle>
            <CardDescription>
              Enter this verification code in your CLI to complete login
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col items-center gap-4">
            <div className="rounded-md border bg-muted px-6 py-4">
              <code className="text-3xl font-mono font-bold tracking-widest select-all">
                {deviceCode}
              </code>
            </div>
            <p className="text-xs text-muted-foreground text-center">
              Copy and paste this code into your terminal, then you can close this window.
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <CardTitle className="text-2xl">CLI Login Request</CardTitle>
          <CardDescription>
            A CLI session is requesting access to your account
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-4">
          <div className="rounded-md border p-3 space-y-1">
            <div className="text-sm">
              <span className="text-muted-foreground">Session: </span>
              <span className="font-medium">{tokenName}</span>
            </div>
            <div className="text-sm">
              <span className="text-muted-foreground">Account: </span>
              <span className="font-medium">{session.user.email}</span>
            </div>
          </div>

          <p className="text-xs text-muted-foreground">
            By approving, you grant the CLI full admin access to manage this
            instance. Only approve if you initiated this login request.
          </p>
        </CardContent>

        <CardFooter className="flex gap-3">
          <Button
            variant="outline"
            className="flex-1"
            onClick={() => window.close()}
            disabled={submitting}
          >
            Deny
          </Button>
          <Button
            className="flex-1"
            onClick={handleApprove}
            disabled={submitting}
          >
            {submitting ? "Approving..." : "Approve"}
          </Button>
        </CardFooter>
      </Card>
    </div>
  );
}

import { useState } from "react";
import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";

const providers = [
  { id: "google" as const, label: "Sign in with Google" },
  { id: "github" as const, label: "Sign in with GitHub" },
  { id: "microsoft" as const, label: "Sign in with Microsoft" },
];

export function ProviderButtons() {
  const [loadingProvider, setLoadingProvider] = useState<string | null>(null);

  async function handleSocialSignIn(provider: "google" | "github" | "microsoft") {
    setLoadingProvider(provider);
    try {
      await authClient.signIn.social({
        provider,
        callbackURL: "/",
      });
    } catch {
      setLoadingProvider(null);
    }
  }

  return (
    <div className="flex flex-col gap-2">
      {providers.map(({ id, label }) => (
        <Button
          key={id}
          variant="outline"
          className="w-full"
          disabled={loadingProvider !== null}
          onClick={() => handleSocialSignIn(id)}
        >
          {loadingProvider === id ? "Redirecting..." : label}
        </Button>
      ))}
    </div>
  );
}

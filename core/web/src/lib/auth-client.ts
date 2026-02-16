import { createAuthClient } from "better-auth/react";
import { adminClient } from "better-auth/client/plugins";
import { BASE_PATH } from "./config";

export const authClient = createAuthClient({
  baseURL: import.meta.env.VITE_API_URL || BASE_PATH,
  plugins: [adminClient()],
});

export const { useSession } = authClient;

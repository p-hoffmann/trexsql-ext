import { betterAuth } from "better-auth";
import { admin, jwt, oidcProvider } from "better-auth/plugins";
import { Pool } from "pg";

const databaseUrl = Deno.env.get("DATABASE_URL");
if (!databaseUrl) {
  throw new Error("DATABASE_URL environment variable is required");
}

const pool = new Pool({
  connectionString: databaseUrl,
  options: "-c search_path=trex,public",
});

export const auth = betterAuth({
  database: pool,
  basePath: "/api/auth",
  secret: Deno.env.get("BETTER_AUTH_SECRET"),
  baseURL: Deno.env.get("BETTER_AUTH_URL") || "http://localhost:8000",
  trustedOrigins: (Deno.env.get("BETTER_AUTH_TRUSTED_ORIGINS") || "http://localhost:5173").split(","),

  emailAndPassword: {
    enabled: true,
    requireEmailVerification: false,
    sendResetPassword: async ({ user, url }) => {
      console.log(`[auth] Password reset for ${user.email}: ${url}`);
    },
  },

  emailVerification: {
    sendVerificationEmail: async ({ user, url }) => {
      console.log(`[auth] Email verification for ${user.email}: ${url}`);
    },
    sendOnSignUp: true,
  },

  socialProviders: {
    ...(Deno.env.get("GOOGLE_CLIENT_ID") && {
      google: {
        clientId: Deno.env.get("GOOGLE_CLIENT_ID")!,
        clientSecret: Deno.env.get("GOOGLE_CLIENT_SECRET")!,
      },
    }),
    ...(Deno.env.get("GITHUB_CLIENT_ID") && {
      github: {
        clientId: Deno.env.get("GITHUB_CLIENT_ID")!,
        clientSecret: Deno.env.get("GITHUB_CLIENT_SECRET")!,
      },
    }),
    ...(Deno.env.get("MICROSOFT_CLIENT_ID") && {
      microsoft: {
        clientId: Deno.env.get("MICROSOFT_CLIENT_ID")!,
        clientSecret: Deno.env.get("MICROSOFT_CLIENT_SECRET")!,
      },
    }),
  },

  account: {
    accountLinking: {
      enabled: true,
    },
  },

  session: {
    expiresIn: 7 * 24 * 60 * 60, // 7 days
  },

  user: {
    additionalFields: {
      role: {
        type: "string",
        defaultValue: "user",
      },
      deletedAt: {
        type: "string",
        required: false,
      },
    },
  },

  databaseHooks: {
    user: {
      create: {
        after: async (user) => {
          const adminEmail = Deno.env.get("ADMIN_EMAIL");
          const client = await pool.connect();
          try {
            const result = await client.query(
              'SELECT COUNT(*)::int AS count FROM "user"'
            );
            const userCount = result.rows[0].count;
            const isFirstUser = userCount <= 1;
            const matchesAdminEmail =
              adminEmail && user.email === adminEmail;

            if (isFirstUser || matchesAdminEmail) {
              await client.query(
                'UPDATE "user" SET role = $1 WHERE id = $2',
                ["admin", user.id]
              );
              console.log(
                `[auth] Assigned admin role to ${user.email} (${isFirstUser ? "first user" : "ADMIN_EMAIL match"})`
              );
            }
          } finally {
            client.release();
          }
        },
      },
    },
  },

  plugins: [
    admin({
      adminRoles: ["admin"],
      defaultRole: "user",
    }),
    jwt(),
    oidcProvider({
      loginPage: "/login",
      consentPage: "/consent",
    }),
  ],
});

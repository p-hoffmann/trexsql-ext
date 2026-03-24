import type { Request, Response, NextFunction } from "express";
import { Pool } from "pg";
import { verifyAccessToken } from "../auth/jwt.ts";

let pool: InstanceType<typeof Pool> | null = null;

function getPool(): InstanceType<typeof Pool> {
  if (!pool) {
    const databaseUrl = Deno.env.get("DATABASE_URL");
    if (databaseUrl) {
      pool = new Pool({
        connectionString: databaseUrl,
        options: "-c search_path=trex,public",
      });
    }
  }
  return pool!;
}

export async function authContext(
  req: Request,
  _res: Response,
  next: NextFunction,
) {
  try {
    // 1. Check Bearer token, ?token= query param, or sb-access-token cookie
    const authHeader = req.headers.authorization;
    const queryToken = req.query?.token as string | undefined;
    const cookieToken = req.headers.cookie
      ?.split(";")
      .map((c) => c.trim())
      .find((c) => c.startsWith("sb-access-token="))
      ?.split("=")
      .slice(1)
      .join("=");
    const bearerToken = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : (queryToken || cookieToken);
    if (bearerToken) {
      const token = bearerToken;
      const claims = await verifyAccessToken(token);
      if (claims) {
        const trexRole = claims.app_metadata?.trex_role || "user";
        (req as any).pgSettings = {
          "app.user_id": claims.sub,
          "app.user_role": claims.role === "service_role" ? "admin" : trexRole,
          "request.jwt.claims": JSON.stringify(claims),
        };

        // Fetch application roles
        try {
          const p = getPool();
          if (p) {
            const result = await p.query(
              `SELECT r.name FROM trex.user_role ur
               JOIN trex.role r ON ur."roleId" = r.id
               WHERE ur."userId" = $1`,
              [claims.sub],
            );
            (req as any).applicationRoles = result.rows.map(
              (row: any) => row.name,
            );
          } else {
            (req as any).applicationRoles = [];
          }
        } catch {
          (req as any).applicationRoles = [];
        }

        return next();
      }
    }

    // 2. Check apikey header (anon/service_role)
    const apikey = req.headers.apikey as string | undefined;
    if (apikey) {
      const claims = await verifyAccessToken(apikey);
      if (claims?.role === "anon") {
        (req as any).pgSettings = {
          "request.jwt.claims": JSON.stringify(claims),
        };
        (req as any).applicationRoles = [];
        return next();
      }
      if (claims?.role === "service_role") {
        (req as any).pgSettings = {
          "app.user_role": "admin",
          "request.jwt.claims": JSON.stringify(claims),
        };
        (req as any).applicationRoles = [];
        return next();
      }
    }

    // 3. No valid auth — set empty context
    (req as any).pgSettings = {};
    (req as any).applicationRoles = [];
  } catch {
    (req as any).pgSettings = {};
    (req as any).applicationRoles = [];
  }

  next();
}

export const TREX_PORT = 8000;
export const POSTGREST_PORT = 3000;
export const POSTGREST_IMAGE = "postgrest/postgrest:v12.2.3";

export interface ContainerEnvVars {
  DATABASE_URL: string;
  BETTER_AUTH_SECRET: string;
  BETTER_AUTH_URL: string;
  BASE_PATH: string;
  POSTGREST_HOST: string;
  POSTGREST_PORT: string;
  SCHEMA_DIR: string;
  PLUGINS_PATH: string;
  PLUGINS_DEV_PATH: string;
  SWARM_CONFIG: string;
  SWARM_NODE: string;
}

export function buildSwarmConfig(port: number, tlsPort: number): string {
  return JSON.stringify({
    cluster_id: "cloud",
    nodes: {
      cloud: {
        gossip_addr: "0.0.0.0:4200",
        extensions: [
          {
            name: "trexas",
            config: {
              host: "0.0.0.0",
              port,
              main_service_path: "/usr/src/core/server",
              event_worker_path: "/usr/src/core/event",
              tls_port: tlsPort,
              tls_cert_path: "/usr/src/server.crt",
              tls_key_path: "/usr/src/server.key",
            },
          },
          {
            name: "pgwire",
            config: { host: "0.0.0.0", port: 5432 },
          },
        ],
      },
    },
  });
}

export function buildTrexEnvVars(opts: {
  databaseUrl: string;
  authSecret: string;
  endpointUrl: string;
}): Record<string, string> {
  return {
    DATABASE_URL: opts.databaseUrl,
    BETTER_AUTH_SECRET: opts.authSecret,
    BETTER_AUTH_URL: `${opts.endpointUrl}/trex`,
    BASE_PATH: "/trex",
    POSTGREST_HOST: "localhost",
    POSTGREST_PORT: String(POSTGREST_PORT),
    SCHEMA_DIR: "/usr/src/core/schema",
    PLUGINS_PATH: "/usr/src/plugins",
    PLUGINS_DEV_PATH: "/usr/src/plugins-dev",
    SWARM_CONFIG: buildSwarmConfig(8001, TREX_PORT),
    SWARM_NODE: "cloud",
  };
}

export function buildPostgrestEnvVars(opts: {
  databaseUrl: string;
  jwtSecret: string;
  endpointUrl: string;
}): Record<string, string> {
  // PostgREST needs an authenticator role connection
  const dbUrl = opts.databaseUrl.replace(
    /\/\/[^@]+@/,
    "//authenticator:authenticator_pass@"
  );
  return {
    PGRST_DB_URI: dbUrl,
    PGRST_DB_SCHEMAS: "public",
    PGRST_DB_ANON_ROLE: "anon",
    PGRST_JWT_SECRET: opts.jwtSecret,
    PGRST_DB_PRE_REQUEST: "public.postgrest_pre_request",
    PGRST_DB_USE_LEGACY_GUCS: "false",
    PGRST_OPENAPI_SERVER_PROXY_URI: `${opts.endpointUrl}/trex/rest/v1`,
  };
}

export const TREX_HEALTH_CHECK = {
  path: "/trex/health",
  port: TREX_PORT,
  intervalSeconds: 30,
  timeoutSeconds: 10,
  healthyThreshold: 2,
  unhealthyThreshold: 5,
};

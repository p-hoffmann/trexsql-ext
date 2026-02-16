import { makeExtendSchemaPlugin, gql } from "graphile-utils";
import { Plugins } from "../plugin/plugin.ts";
import { scanDiskPlugins } from "../routes/plugin.ts";
import { reloadAuthProviders } from "../auth.ts";

declare const Trex: any;
declare const Deno: any;

function escapeSql(s: string): string {
  return s.replace(/'/g, "''");
}

function assertAdmin(context: any) {
  const role = context.pgSettings?.["app.user_role"];
  if (role !== "admin") {
    throw new Error("Forbidden");
  }
}

export const pluginOperationsPlugin = makeExtendSchemaPlugin(() => ({
    typeDefs: gql`
      type PluginInfo {
        name: String!
        version: String
        activeVersion: String
        active: Boolean!
        installed: Boolean!
        pendingRestart: Boolean!
        description: String
        registryVersion: String
      }

      type PluginResult {
        success: Boolean!
        error: String
        results: JSON
      }

      type TestConnectionResult {
        success: Boolean!
        message: String!
      }

      type TrexNode {
        nodeId: String!
        nodeName: String!
        gossipAddr: String!
        dataNode: String!
        status: String!
      }

      type TrexService {
        nodeName: String!
        serviceName: String!
        host: String!
        port: String!
        status: String!
        uptimeSeconds: String!
        config: String
      }

      type TrexClusterStatus {
        totalNodes: String!
        activeQueries: String!
        queuedQueries: String!
        memoryUtilizationPct: String!
      }

      type ServiceActionResult {
        success: Boolean!
        message: String
        error: String
      }

      type TrexDatabase {
        databaseName: String!
        databaseOid: String!
        path: String
        internal: Boolean!
        type: String!
      }

      type TrexSchema {
        databaseName: String!
        schemaName: String!
        schemaOid: String!
        internal: Boolean!
      }

      type TrexTable {
        databaseName: String!
        schemaName: String!
        tableName: String!
        tableOid: String!
        internal: Boolean!
        hasPrimaryKey: Boolean!
        estimatedSize: String!
        columnCount: String!
        indexCount: String!
        temporary: Boolean!
      }

      type TrexExtension {
        extensionName: String!
        loaded: Boolean!
        installed: Boolean!
        extensionVersion: String
        description: String
        installPath: String
      }

      type EtlPipeline {
        name: String!
        state: String!
        mode: String!
        connection: String!
        publication: String!
        snapshot: String!
        rowsReplicated: String!
        lastActivity: String!
        error: String
      }

      type EtlActionResult {
        success: Boolean!
        message: String
        error: String
      }

      extend type Query {
        availablePlugins: [PluginInfo!]!
        trexNodes: [TrexNode!]!
        trexServices: [TrexService!]!
        trexClusterStatus: TrexClusterStatus
        trexDatabases: [TrexDatabase!]!
        trexSchemas: [TrexSchema!]!
        trexTables(database: String, schema: String): [TrexTable!]!
        trexExtensions: [TrexExtension!]!
        etlPipelines: [EtlPipeline!]!
      }

      extend type Mutation {
        installPlugin(packageSpec: String!): PluginResult!
        uninstallPlugin(packageName: String!): PluginResult!
        updatePlugin(packageName: String!, version: String): PluginResult!
        testDatabaseConnection(databaseId: String!): TestConnectionResult!
        reloadSsoProviders: Boolean!
        startService(extension: String!, config: String!): ServiceActionResult!
        stopService(extension: String!): ServiceActionResult!
        startEtlPipeline(
          name: String!
          databaseId: String!
          mode: String!
          publication: String
          schema: String
          batchSize: Int
          batchTimeoutMs: Int
          retryDelayMs: Int
          retryMaxAttempts: Int
        ): EtlActionResult!
        stopEtlPipeline(name: String!): EtlActionResult!
      }
    `,
    resolvers: {
      Query: {
        async availablePlugins(_parent: any, _args: any, context: any) {
          assertAdmin(context);

          const diskPlugins = await scanDiskPlugins();
          const activePlugins = Plugins.getActivePlugins();

          const pluginList: any[] = [];
          const seen = new Set<string>();

          for (const [name, diskInfo] of diskPlugins) {
            seen.add(name);
            const activeEntry = activePlugins.get(name);
            const active = !!activeEntry;
            const activeVersion = activeEntry?.version || null;
            const pendingRestart = !active || activeVersion !== diskInfo.version;
            pluginList.push({
              name,
              version: diskInfo.version,
              activeVersion,
              active,
              installed: true,
              pendingRestart,
            });
          }

          for (const [name, activeEntry] of activePlugins) {
            if (seen.has(name)) continue;
            pluginList.push({
              name,
              version: null,
              activeVersion: activeEntry.version,
              active: true,
              installed: false,
              pendingRestart: true,
            });
          }

          // Enrich with registry info if configured
          const registryUrl = Deno.env.get("PLUGINS_INFORMATION_URL");
          if (registryUrl) {
            try {
              const pkgsRes = await fetch(registryUrl);
              const pkgsJson = await pkgsRes.json();
              const packages = pkgsJson.value || pkgsJson;

              const registryMap = new Map<string, { description: string; registryVersion: string }>();

              for (const pkg of packages) {
                const pkgname = pkg.name?.replace(/@[^/]+\//, "") || pkg.name;
                let bestVersion = { version: "", packageDescription: "" };
                if (pkg.versions && Array.isArray(pkg.versions)) {
                  bestVersion = pkg.versions.reduce((m: any, c: any) => {
                    return c.version > m.version ? c : m;
                  }, bestVersion);
                }
                registryMap.set(pkgname, {
                  description: bestVersion.packageDescription || pkg.description || "",
                  registryVersion: bestVersion.version || pkg.version || "",
                });
              }

              for (const plugin of pluginList) {
                const regInfo = registryMap.get(plugin.name);
                if (regInfo) {
                  plugin.description = regInfo.description;
                  plugin.registryVersion = regInfo.registryVersion;
                }
              }

              for (const [pkgname, regInfo] of registryMap) {
                if (!seen.has(pkgname) && !activePlugins.has(pkgname)) {
                  pluginList.push({
                    name: pkgname,
                    version: null,
                    activeVersion: null,
                    active: false,
                    installed: false,
                    pendingRestart: false,
                    description: regInfo.description,
                    registryVersion: regInfo.registryVersion,
                  });
                }
              }
            } catch (e) {
              console.error(`Failed to fetch registry info: ${e}`);
            }
          }

          return pluginList;
        },

        async trexNodes(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_db_nodes()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              nodeId: r.node_id || r[0] || "",
              nodeName: r.node_name || r[1] || "",
              gossipAddr: r.gossip_addr || r[2] || "",
              dataNode: r.data_node || r[3] || "",
              status: r.status || r[4] || "",
            }));
          } catch (err: any) {
            console.error("trexNodes error:", err);
            return [];
          }
        },

        async trexServices(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_db_services()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              nodeName: r.node_name || r[0] || "",
              serviceName: r.service_name || r[1] || "",
              host: r.host || r[2] || "",
              port: r.port || r[3] || "",
              status: r.status || r[4] || "",
              uptimeSeconds: r.uptime_seconds || r[5] || "",
              config: r.config || r[6] || null,
            }));
          } catch (err: any) {
            console.error("trexServices error:", err);
            return [];
          }
        },

        async trexClusterStatus(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_db_cluster_status()", []);
            const rows = result?.rows || result || [];
            if (rows.length === 0) return null;
            const r = rows[0];
            return {
              totalNodes: r.total_nodes || r[0] || "",
              activeQueries: r.active_queries || r[1] || "",
              queuedQueries: r.queued_queries || r[2] || "",
              memoryUtilizationPct: r.memory_utilization_pct || r[3] || "",
            };
          } catch (err: any) {
            console.error("trexClusterStatus error:", err);
            return null;
          }
        },

        async trexDatabases(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(
              "SELECT database_name, database_oid, path, internal, type FROM duckdb_databases()", []
            );
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              databaseName: r.database_name || r[0] || "",
              databaseOid: String(r.database_oid ?? r[1] ?? ""),
              path: r.path || r[2] || null,
              internal: r.internal ?? r[3] ?? false,
              type: r.type || r[4] || "",
            }));
          } catch (err: any) {
            console.error("trexDatabases error:", err);
            return [];
          }
        },

        async trexSchemas(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(
              "SELECT * FROM duckdb_schemas()", []
            );
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              databaseName: r.database_name || r[0] || "",
              schemaName: r.schema_name || r[1] || "",
              schemaOid: String(r.database_oid ?? r.schema_oid ?? r[2] ?? ""),
              internal: r.internal ?? r[3] ?? false,
            }));
          } catch (err: any) {
            console.error("trexSchemas error:", err);
            return [];
          }
        },

        async trexTables(_parent: any, args: { database?: string; schema?: string }, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            let sql = "SELECT * FROM duckdb_tables()";
            const conditions: string[] = [];
            if (args.database) {
              conditions.push(`database_name = '${escapeSql(args.database)}'`);
            }
            if (args.schema) {
              conditions.push(`schema_name = '${escapeSql(args.schema)}'`);
            }
            if (conditions.length > 0) {
              sql += " WHERE " + conditions.join(" AND ");
            }
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              databaseName: r.database_name || r[0] || "",
              schemaName: r.schema_name || r[1] || "",
              tableName: r.table_name || r[2] || "",
              tableOid: String(r.table_oid ?? r[3] ?? ""),
              internal: r.internal ?? r[4] ?? false,
              hasPrimaryKey: r.has_primary_key ?? r[5] ?? false,
              estimatedSize: String(r.estimated_size ?? r[6] ?? ""),
              columnCount: String(r.column_count ?? r[7] ?? ""),
              indexCount: String(r.index_count ?? r[8] ?? ""),
              temporary: r.temporary ?? r[9] ?? false,
            }));
          } catch (err: any) {
            console.error("trexTables error:", err);
            return [];
          }
        },

        async trexExtensions(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM duckdb_extensions()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              extensionName: r.extension_name || r[0] || "",
              loaded: r.loaded ?? r[1] ?? false,
              installed: r.installed ?? r[2] ?? false,
              extensionVersion: r.extension_version || r[3] || null,
              description: r.description || r[4] || null,
              installPath: r.install_path || r[5] || null,
            }));
          } catch (err: any) {
            console.error("trexExtensions error:", err);
            return [];
          }
        },

        async etlPipelines(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_etl_status()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              state: r.state || r[1] || "",
              mode: r.mode || r[2] || "",
              connection: r.connection || r[3] || "",
              publication: r.publication || r[4] || "",
              snapshot: r.snapshot || r[5] || "",
              rowsReplicated: r.rows_replicated || r[6] || "0",
              lastActivity: r.last_activity || r[7] || "",
              error: r.error || r[8] || null,
            }));
          } catch (err: any) {
            console.error("etlPipelines error:", err);
            return [];
          }
        },
      },
      Mutation: {
        async installPlugin(_parent: any, args: { packageSpec: string }, context: any) {
          assertAdmin(context);

          const { packageSpec } = args;
          if (!packageSpec) {
            return { success: false, error: "packageSpec is required", results: null };
          }

          try {
            const dir = Deno.env.get("PLUGINS_PATH") || "./plugins";
            const sql = `SELECT install_results FROM trex_plugin_install_with_deps('${escapeSql(packageSpec)}', '${escapeSql(dir)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const parsed = rows.map((r: any) => {
              try {
                return JSON.parse(r.install_results || r[0]);
              } catch {
                return r.install_results || r[0];
              }
            });
            return { success: true, error: null, results: parsed.length === 1 ? parsed[0] : parsed };
          } catch (err: any) {
            console.error("Plugin install error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async uninstallPlugin(_parent: any, args: { packageName: string }, context: any) {
          assertAdmin(context);

          const { packageName } = args;
          if (!packageName) {
            return { success: false, error: "packageName is required", results: null };
          }

          try {
            const dir = Deno.env.get("PLUGINS_PATH") || "./plugins";
            const sql = `SELECT delete_results FROM trex_plugin_delete('${escapeSql(packageName)}', '${escapeSql(dir)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const parsed = rows.map((r: any) => {
              try {
                return JSON.parse(r.delete_results || r[0]);
              } catch {
                return r.delete_results || r[0];
              }
            });
            return { success: true, error: null, results: parsed[0] || null };
          } catch (err: any) {
            console.error("Plugin uninstall error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async updatePlugin(_parent: any, args: { packageName: string; version?: string }, context: any) {
          assertAdmin(context);

          const { packageName, version } = args;
          if (!packageName) {
            return { success: false, error: "packageName is required", results: null };
          }

          try {
            const dir = Deno.env.get("PLUGINS_PATH") || "./plugins";
            const conn = new Trex.TrexDB("memory");

            // Delete first
            const delSql = `SELECT delete_results FROM trex_plugin_delete('${escapeSql(packageName)}', '${escapeSql(dir)}')`;
            await conn.execute(delSql, []);

            // Reinstall
            const spec = version ? `${packageName}@${escapeSql(version)}` : packageName;
            const installSql = `SELECT install_results FROM trex_plugin_install_with_deps('${escapeSql(spec)}', '${escapeSql(dir)}')`;
            const result = await conn.execute(installSql, []);
            const rows = result?.rows || result || [];
            const parsed = rows.map((r: any) => {
              try {
                return JSON.parse(r.install_results || r[0]);
              } catch {
                return r.install_results || r[0];
              }
            });
            return { success: true, error: null, results: parsed.length === 1 ? parsed[0] : parsed };
          } catch (err: any) {
            console.error("Plugin update error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async testDatabaseConnection(_parent: any, args: { databaseId: string }, context: any) {
          assertAdmin(context);

          const { databaseId } = args;
          if (!databaseId) {
            return { success: false, message: "databaseId is required" };
          }

          const pg = (await import("pg")).default;
          const mainPool = new pg.Pool({
            connectionString: Deno.env.get("DATABASE_URL"),
          });

          try {
            const dbResult = await mainPool.query(
              `SELECT host, port, "databaseName", dialect FROM trex.database WHERE id = $1`,
              [databaseId]
            );
            if (dbResult.rows.length === 0) {
              return { success: false, message: "Database not found" };
            }

            const db = dbResult.rows[0];

            const credResult = await mainPool.query(
              `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
              [databaseId]
            );
            if (credResult.rows.length === 0) {
              return { success: false, message: "No credentials configured" };
            }

            const cred = credResult.rows[0];

            const testPool = new pg.Pool({
              host: db.host,
              port: db.port,
              database: db.databaseName,
              user: cred.username,
              password: cred.password,
              connectionTimeoutMillis: 5000,
              max: 1,
            });

            try {
              const client = await testPool.connect();
              await client.query("SELECT 1");
              client.release();
              return { success: true, message: "Connection successful" };
            } catch (connErr: any) {
              return { success: false, message: connErr.message || "Connection failed" };
            } finally {
              await testPool.end();
            }
          } catch (err: any) {
            console.error("Test connection error:", err);
            return { success: false, message: "Internal server error" };
          } finally {
            await mainPool.end();
          }
        },

        async reloadSsoProviders(_parent: any, _args: any, context: any) {
          assertAdmin(context);

          try {
            await reloadAuthProviders();
            return true;
          } catch (err: any) {
            console.error("SSO reload error:", err);
            throw new Error(err.message || "Failed to reload SSO providers");
          }
        },

        async startService(_parent: any, args: { extension: string; config: string }, context: any) {
          assertAdmin(context);
          const { extension, config } = args;
          try {
            const sql = `SELECT trex_db_start_service('${escapeSql(extension)}', '${escapeSql(config)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_db_start_service || "Service started";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("startService error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },

        async stopService(_parent: any, args: { extension: string }, context: any) {
          assertAdmin(context);
          const { extension } = args;
          try {
            const sql = `SELECT trex_db_stop_service('${escapeSql(extension)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_db_stop_service || "Service stopped";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("stopService error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },

        async startEtlPipeline(
          _parent: any,
          args: {
            name: string;
            databaseId: string;
            mode: string;
            publication?: string;
            schema?: string;
            batchSize?: number;
            batchTimeoutMs?: number;
            retryDelayMs?: number;
            retryMaxAttempts?: number;
          },
          context: any,
        ) {
          assertAdmin(context);

          const { name, databaseId, mode } = args;
          const validModes = ["copy_and_cdc", "cdc_only", "copy_only"];
          if (!validModes.includes(mode)) {
            return { success: false, message: null, error: `Invalid mode '${mode}'. Must be one of: ${validModes.join(", ")}` };
          }
          if ((mode === "copy_and_cdc" || mode === "cdc_only") && !args.publication) {
            return { success: false, message: null, error: "Publication name is required for CDC modes" };
          }

          const pg = (await import("pg")).default;
          const mainPool = new pg.Pool({
            connectionString: Deno.env.get("DATABASE_URL"),
          });

          try {
            const dbResult = await mainPool.query(
              `SELECT host, port, "databaseName" FROM trex.database WHERE id = $1`,
              [databaseId]
            );
            if (dbResult.rows.length === 0) {
              return { success: false, message: null, error: "Database not found" };
            }
            const db = dbResult.rows[0];

            const credResult = await mainPool.query(
              `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
              [databaseId]
            );
            if (credResult.rows.length === 0) {
              return { success: false, message: null, error: "No credentials configured for this database" };
            }
            const cred = credResult.rows[0];

            // Build libpq connection string
            const parts: string[] = [
              `host='${escapeSql(db.host)}'`,
              `port='${escapeSql(String(db.port))}'`,
              `dbname='${escapeSql(db.databaseName)}'`,
              `user='${escapeSql(cred.username)}'`,
              `password='${escapeSql(cred.password)}'`,
            ];

            if (mode === "copy_and_cdc" || mode === "cdc_only") {
              parts.push(`publication='${escapeSql(args.publication!)}'`);
            }
            if (mode === "copy_only") {
              const schemaName = args.schema || "public";
              parts.push(`schema='${escapeSql(schemaName)}'`);
            }

            const connStr = parts.join(" ");

            // Build SQL call
            let sql: string;
            if (args.batchSize != null || args.batchTimeoutMs != null || args.retryDelayMs != null || args.retryMaxAttempts != null) {
              const batchSize = args.batchSize ?? 1000;
              const batchTimeout = args.batchTimeoutMs ?? 5000;
              const retryDelay = args.retryDelayMs ?? 10000;
              const retryMax = args.retryMaxAttempts ?? 5;
              sql = `SELECT trex_etl_start('${escapeSql(name)}', '${escapeSql(connStr)}', '${escapeSql(mode)}', ${batchSize}, ${batchTimeout}, ${retryDelay}, ${retryMax})`;
            } else {
              sql = `SELECT trex_etl_start('${escapeSql(name)}', '${escapeSql(connStr)}', '${escapeSql(mode)}')`;
            }

            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_etl_start || "Pipeline started";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("startEtlPipeline error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          } finally {
            await mainPool.end();
          }
        },

        async stopEtlPipeline(_parent: any, args: { name: string }, context: any) {
          assertAdmin(context);
          const { name } = args;
          try {
            const sql = `SELECT trex_etl_stop('${escapeSql(name)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_etl_stop || "Pipeline stopped";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("stopEtlPipeline error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },
      },
    },
}));

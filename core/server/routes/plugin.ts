import type { Express, Request, Response } from "express";
import { Plugins } from "../plugin/plugin.ts";
import { getPluginsJson } from "../plugin/ui.ts";
import { authContext } from "../middleware/auth-context.ts";

function checkSemver(version: string, sver: string): boolean {
  if (!sver || sver === "latest" || sver === "all") return true;
  // Simple semver range check: if version starts with sver major
  // For full semver support, a library would be needed
  try {
    const vParts = version.split(".");
    const sParts = sver.replace(/[^\d.]/g, "").split(".");
    if (sParts[0] && vParts[0] !== sParts[0]) return false;
    return true;
  } catch (_) {
    return true;
  }
}

async function scanDiskPlugins(): Promise<
  Map<string, { name: string; version: string }>
> {
  const diskPlugins = new Map<string, { name: string; version: string }>();
  const pluginsPath = Deno.env.get("PLUGINS_PATH") || "./plugins";

  async function scanDir(dir: string) {
    for await (const entry of Deno.readDir(dir)) {
      if (!entry.isDirectory) continue;
      if (entry.name.startsWith("@")) {
        await scanDir(`${dir}/${entry.name}`);
        continue;
      }
      try {
        const pkgJsonPath = `${dir}/${entry.name}/package.json`;
        const pkg = JSON.parse(await Deno.readTextFile(pkgJsonPath));
        const shortName = pkg.name?.includes("/")
          ? pkg.name.split("/").pop()
          : pkg.name || entry.name;
        diskPlugins.set(shortName, { name: shortName, version: pkg.version });
      } catch (_e) {
        // Skip entries without valid package.json
      }
    }
  }

  try {
    await scanDir(pluginsPath);
  } catch (_e) {
    // Plugins directory not readable
  }
  return diskPlugins;
}

export function addPluginRoutes(app: Express) {
  // GET /api/plugins — list plugins
  app.get("/api/plugins", authContext, async (req: Request, res: Response) => {
    const pgSettings = (req as any).pgSettings || {};
    if (pgSettings["app.user_role"] !== "admin") {
      res.status(403).json({ error: "Forbidden" });
      return;
    }

    const q = (req.query.version as string) || "compatible";

    try {
      const diskPlugins = await scanDiskPlugins();
      const activePlugins = Plugins.getActivePlugins();

      const pluginList: any[] = [];
      const seen = new Set<string>();

      // Add all on-disk plugins
      for (const [name, diskInfo] of diskPlugins) {
        seen.add(name);
        const activeEntry = activePlugins.get(name);
        const active = !!activeEntry;
        const activeVersion = activeEntry?.version || null;
        const pendingRestart =
          !active || activeVersion !== diskInfo.version;
        pluginList.push({
          name,
          version: diskInfo.version,
          activeVersion,
          active,
          installed: true,
          pendingRestart,
        });
      }

      // Add active plugins not on disk (deleted but still serving)
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

      if (q === "none") {
        res.json(pluginList);
        return;
      }

      // Enrich with registry info if configured
      const registryUrl = Deno.env.get("PLUGINS_INFORMATION_URL");
      if (registryUrl) {
        try {
          const apiVersion =
            Deno.env.get("PLUGINS_API_VERSION") || "latest";
          const pkgsRes = await fetch(registryUrl);
          const pkgsJson = await pkgsRes.json();

          const registryMap = new Map<
            string,
            { description: string; registryVersion: string }
          >();
          const packages = pkgsJson.value || pkgsJson;

          for (const pkg of packages) {
            const pkgname = pkg.name?.replace(/@[^/]+\//, "") || pkg.name;
            let bestVersion = { version: "", packageDescription: "" };
            if (pkg.versions && Array.isArray(pkg.versions)) {
              bestVersion = pkg.versions.reduce((m: any, c: any) => {
                return c.version > m.version &&
                  checkSemver(c.version, q === "compatible" ? apiVersion : q)
                  ? c
                  : m;
              }, bestVersion);
            }
            registryMap.set(pkgname, {
              description: bestVersion.packageDescription || pkg.description || "",
              registryVersion: bestVersion.version || pkg.version || "",
            });
          }

          // Merge registry info into plugin list
          for (const plugin of pluginList) {
            const regInfo = registryMap.get(plugin.name);
            if (regInfo) {
              plugin.description = regInfo.description;
              plugin.registryVersion = regInfo.registryVersion;
            }
          }

          // Add registry-only plugins
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

      res.json(pluginList);
    } catch (err) {
      console.error("Plugin list error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  // GET /api/plugins/ui — return merged UI plugins JSON
  app.get("/api/plugins/ui", (_req: Request, res: Response) => {
    res.setHeader("Content-Type", "application/json");
    res.send(getPluginsJson());
  });
}

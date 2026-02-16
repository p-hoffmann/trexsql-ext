import type { Express } from "express";
import { addPlugin as addFlowPlugin } from "./flow.ts";
import { addPlugin as addFunctionPlugin } from "./function.ts";
import { addMigrationPlugin } from "./migration.ts";
import { addPlugin as addUIPlugin } from "./ui.ts";

interface ActivePluginEntry {
  name: string;
  version: string;
  registeredAt: Date;
}

export class Plugins {
  static activeRegistry: Map<string, ActivePluginEntry> = new Map();

  private static addPlugin(
    app: Express,
    dir: string,
    pkg: any,
    shortName: string
  ) {
    try {
      if (!pkg.trex) {
        console.log(
          `Plugin ${shortName} has no trex config — skipping registration`
        );
        return;
      }
      for (const [key, value] of Object.entries(pkg.trex)) {
        switch (key) {
          case "functions":
            addFunctionPlugin(app, value, dir, shortName);
            break;
          case "ui":
            addUIPlugin(app, value, dir);
            break;
          case "flow":
            addFlowPlugin(value);
            break;
          case "migrations":
            addMigrationPlugin(value, dir, shortName);
            break;
          default:
            console.log(`Unknown plugin type: ${key}`);
        }
      }
      Plugins.activeRegistry.set(shortName, {
        name: shortName,
        version: pkg.version,
        registeredAt: new Date(),
      });
    } catch (e) {
      console.error(`Failed to register plugin ${shortName}:`, e);
    }
  }

  private static async scanAndRegister(
    app: Express,
    dir: string,
    versionSuffix?: string
  ) {
    async function scanLevel(scanDir: string) {
      for await (const entry of Deno.readDir(scanDir)) {
        if (!entry.isDirectory) continue;
        if (entry.name.startsWith("@")) {
          await scanLevel(`${scanDir}/${entry.name}`);
          continue;
        }
        try {
          const pkgJsonPath = `${scanDir}/${entry.name}/package.json`;
          const pkg = JSON.parse(await Deno.readTextFile(pkgJsonPath));
          if (versionSuffix) {
            pkg.version = pkg.version + versionSuffix;
          }
          const shortName = pkg.name?.includes("/")
            ? pkg.name.split("/").pop()
            : pkg.name || entry.name;
          console.log(
            `Found plugin ${shortName} (v${pkg.version}) in ${scanDir}`
          );
          Plugins.addPlugin(app, `${scanDir}/${entry.name}`, pkg, shortName);
          console.log(`Registered plugin ${shortName}`);
        } catch (_e) {
          console.log(
            `${entry.name} does not have a valid package.json — skipped`
          );
        }
      }
    }

    try {
      await scanLevel(dir);
    } catch (_e) {
      console.log(
        `Plugins directory ${dir} not found or not readable — skipping`
      );
    }
  }

  static async initPlugins(app: Express) {
    const pluginsPath = Deno.env.get("PLUGINS_PATH") || "./plugins";
    console.log("Scanning and registering plugins");

    await Plugins.scanAndRegister(app, pluginsPath);

    // Scan dev plugins directory in development mode
    if (Deno.env.get("NODE_ENV") === "development") {
      const devPath = Deno.env.get("PLUGINS_DEV_PATH") || "./plugins-dev";
      await Plugins.scanAndRegister(app, devPath, "-dev");
    }

    console.log(
      `Plugin registration complete: ${Plugins.activeRegistry.size} plugins active`
    );
  }

  static getActivePlugins(): Map<string, ActivePluginEntry> {
    return Plugins.activeRegistry;
  }
}

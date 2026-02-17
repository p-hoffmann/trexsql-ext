import type { Express } from "express";
import { addPlugin as addFlowPlugin } from "./flow.ts";
import { addPlugin as addFunctionPlugin } from "./function.ts";
import { addMigrationPlugin } from "./migration.ts";
import { addTransformPlugin } from "./transform.ts";
import { addPlugin as addUIPlugin } from "./ui.ts";
import { scanPluginDirectory } from "./utils.ts";

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
    fullName: string
  ) {
    try {
      if (!pkg.trex) {
        console.log(
          `Plugin ${fullName} has no trex config — skipping registration`
        );
        return;
      }
      for (const [key, value] of Object.entries(pkg.trex)) {
        switch (key) {
          case "functions":
            addFunctionPlugin(app, value, dir, fullName);
            break;
          case "ui":
            addUIPlugin(app, value, dir, fullName);
            break;
          case "flow":
            addFlowPlugin(value);
            break;
          case "migrations":
            addMigrationPlugin(value, dir, fullName);
            break;
          case "transform":
            addTransformPlugin(app, value, dir, shortName);
            break;
          default:
            console.log(`Unknown plugin type: ${key}`);
        }
      }
      Plugins.activeRegistry.set(fullName, {
        name: fullName,
        version: pkg.version,
        registeredAt: new Date(),
      });
    } catch (e) {
      console.error(`Failed to register plugin ${fullName}:`, e);
    }
  }

  private static async scanAndRegister(
    app: Express,
    dir: string,
    versionSuffix?: string
  ) {
    const scanned = await scanPluginDirectory(dir);
    for (const { shortName, dir: pluginDir, pkg } of scanned) {
      if (versionSuffix) {
        pkg.version = pkg.version + versionSuffix;
      }
      console.log(
        `Found plugin ${shortName} (v${pkg.version}) in ${pluginDir}`
      );
      Plugins.addPlugin(app, pluginDir, pkg, shortName);
      console.log(`Registered plugin ${shortName}`);
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

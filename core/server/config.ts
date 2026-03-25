const raw = Deno.env.get("BASE_PATH") || "/trex";
export const BASE_PATH = raw.endsWith("/") ? raw.slice(0, -1) : raw;

const rawPlugins = Deno.env.get("PLUGINS_BASE_PATH") || "/plugins";
export const PLUGINS_BASE_PATH = rawPlugins.endsWith("/") ? rawPlugins.slice(0, -1) : rawPlugins;


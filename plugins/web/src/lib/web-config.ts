import { createContext, useContext, useEffect, useState } from "react";
import { BASE_PATH } from "@/lib/config";

export interface NavExtra {
  path: string;
  label: string;
  plugin: string;
}

interface WebConfig {
  navExtra: NavExtra[];
}

const DEFAULT_CONFIG: WebConfig = { navExtra: [] };

export const WebConfigContext = createContext<WebConfig>(DEFAULT_CONFIG);

export function useWebConfig() {
  return useContext(WebConfigContext);
}

export function useFetchedWebConfig(): { config: WebConfig; loaded: boolean } {
  const [state, setState] = useState<{ config: WebConfig; loaded: boolean }>({
    config: DEFAULT_CONFIG,
    loaded: false,
  });

  useEffect(() => {
    let cancelled = false;
    fetch(`${BASE_PATH}/api/web-config`)
      .then((r) => (r.ok ? r.json() : { navExtra: [] }))
      .then((c) => {
        if (cancelled) return;
        const navExtra: NavExtra[] = Array.isArray(c?.navExtra)
          ? c.navExtra.filter(
              (it: unknown): it is NavExtra =>
                !!it &&
                typeof (it as NavExtra).path === "string" &&
                typeof (it as NavExtra).label === "string" &&
                typeof (it as NavExtra).plugin === "string",
            )
          : [];
        setState({ config: { navExtra }, loaded: true });
      })
      .catch(() => {
        if (cancelled) return;
        setState({ config: DEFAULT_CONFIG, loaded: true });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return state;
}

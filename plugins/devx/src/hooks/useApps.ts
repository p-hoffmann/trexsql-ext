import { useState, useEffect, useCallback } from "react";
import type { App } from "@/lib/types";
import * as api from "@/lib/api";

export function useApps() {
  const [apps, setApps] = useState<App[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const data = await api.listApps();
      setApps(data);
    } catch (err) {
      console.error("Failed to load apps:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const create = useCallback(async (name: string, template?: string) => {
    const app = await api.createApp(name, template);
    setApps((prev) => [app, ...prev]);
    return app;
  }, []);

  const remove = useCallback(async (appId: string) => {
    await api.deleteApp(appId);
    setApps((prev) => prev.filter((a) => a.id !== appId));
  }, []);

  const update = useCallback(async (appId: string, data: Partial<App>) => {
    const updated = await api.updateApp(appId, data);
    setApps((prev) => prev.map((a) => (a.id === appId ? updated : a)));
    return updated;
  }, []);

  return { apps, loading, refresh, create, remove, update };
}

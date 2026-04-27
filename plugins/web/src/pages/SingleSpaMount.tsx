import { useEffect, useRef, useState } from "react";
import { mountRootParcel, type Parcel } from "single-spa";

interface SingleSpaMountProps {
  plugin: string;
  basePath?: string;
}

/**
 * Mounts a single-spa parcel for a given plugin.
 * Loads the plugin's entry module and CSS, cleans up on unmount.
 */
export function SingleSpaMount({ plugin, basePath }: SingleSpaMountProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const parcelRef = useRef<Parcel | null>(null);
  const cssLinkRef = useRef<HTMLLinkElement | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;

    async function loadAndMount() {
      try {
        const pluginBase = `/plugins/trex/${plugin}`;

        // Load plugin CSS (extracted by Vite's chunked build)
        const link = document.createElement("link");
        link.rel = "stylesheet";
        link.href = `${pluginBase}/${plugin}-spa.css`;
        link.dataset.singleSpa = plugin;
        document.head.appendChild(link);
        cssLinkRef.current = link;

        const parcelUrl = `${pluginBase}/${plugin}-spa.js`;
        const parcelModule = await import(/* @vite-ignore */ parcelUrl);

        if (cancelled) return;

        parcelRef.current = mountRootParcel(parcelModule, {
          domElement: containerRef.current!,
          basePath,
        });
      } catch (err) {
        if (!cancelled) setError(String(err));
      }
    }

    loadAndMount();

    return () => {
      cancelled = true;
      cssLinkRef.current?.remove();
      cssLinkRef.current = null;
      if (parcelRef.current) {
        parcelRef.current.unmount();
        parcelRef.current = null;
      }
    };
  }, [plugin, basePath]);

  if (error) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        <p>Failed to load {plugin}: {error}</p>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className="w-full border-0"
      style={{ height: "calc(100vh - 3.5rem)" }}
    />
  );
}

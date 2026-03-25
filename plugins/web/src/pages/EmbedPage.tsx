/** Renders an external plugin app inside an iframe that fills the content area. */
export function EmbedPage({ plugin }: { plugin: string }) {
  const src = `/plugins/trex/${plugin}/`;
  return (
    <iframe
      src={src}
      className="w-full border-0"
      style={{ height: "calc(100vh - 3.5rem)" }}
      title={plugin}
    />
  );
}

import { useEffect } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const TREX_EXTENSIONS_QUERY = `
  query TrexExtensions {
    trexExtensions { extensionName loaded installed extensionVersion description installPath }
  }
`;

interface ExtensionRow {
  extensionName: string;
  loaded: boolean;
  installed: boolean;
  extensionVersion: string | null;
  description: string | null;
  installPath: string | null;
}

export function Extensions() {
  const [extResult, reexecuteExts] = useQuery({ query: TREX_EXTENSIONS_QUERY });
  const extensions: ExtensionRow[] = extResult.data?.trexExtensions || [];

  function refetch() {
    reexecuteExts({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetch, 30000);
    return () => clearInterval(interval);
  }, []);

  const loadedCount = extensions.filter((e) => e.loaded).length;

  const columns: Column<ExtensionRow>[] = [
    {
      header: "Extension",
      cell: (row) => <span className="text-sm font-medium">{row.extensionName}</span>,
    },
    {
      header: "Version",
      cell: (row) => (
        <code className="text-xs font-mono">{row.extensionVersion || "-"}</code>
      ),
    },
    {
      header: "Loaded",
      cell: (row) => (
        <Badge variant={row.loaded ? "default" : "secondary"}>
          {row.loaded ? "Yes" : "No"}
        </Badge>
      ),
    },
    {
      header: "Installed",
      cell: (row) => (
        <Badge variant={row.installed ? "default" : "secondary"}>
          {row.installed ? "Yes" : "No"}
        </Badge>
      ),
    },
    {
      header: "Description",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">{row.description || "-"}</span>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Extensions</h2>
          <p className="text-muted-foreground">
            {loadedCount} extension{loadedCount === 1 ? "" : "s"} loaded, {extensions.length} available
          </p>
        </div>
        <Button variant="outline" onClick={refetch}>
          <RefreshCwIcon className="h-4 w-4" />
        </Button>
      </div>

      <DataTable
        columns={columns}
        data={extensions}
        loading={extResult.fetching}
        emptyMessage="No extensions found."
      />
    </div>
  );
}

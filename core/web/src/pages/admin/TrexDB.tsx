import { useState, useEffect } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const TREX_DATABASES_QUERY = `
  query TrexDatabases {
    trexDatabases { databaseName databaseOid path internal type }
  }
`;

const TREX_SCHEMAS_QUERY = `
  query TrexSchemas {
    trexSchemas { databaseName schemaName schemaOid internal }
  }
`;

const TREX_TABLES_QUERY = `
  query TrexTables($database: String, $schema: String) {
    trexTables(database: $database, schema: $schema) {
      databaseName schemaName tableName tableOid internal
      hasPrimaryKey estimatedSize columnCount indexCount temporary
    }
  }
`;

interface DatabaseRow {
  databaseName: string;
  databaseOid: string;
  path: string | null;
  internal: boolean;
  type: string;
}

interface SchemaRow {
  databaseName: string;
  schemaName: string;
  schemaOid: string;
  internal: boolean;
}

interface TableRow {
  databaseName: string;
  schemaName: string;
  tableName: string;
  tableOid: string;
  internal: boolean;
  hasPrimaryKey: boolean;
  estimatedSize: string;
  columnCount: string;
  indexCount: string;
  temporary: boolean;
}

export function TrexDB() {
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);

  const [dbResult, reexecuteDbs] = useQuery({ query: TREX_DATABASES_QUERY });
  const [schemaResult, reexecuteSchemas] = useQuery({ query: TREX_SCHEMAS_QUERY });
  const [tableResult, reexecuteTables] = useQuery({
    query: TREX_TABLES_QUERY,
    variables: { database: selectedDatabase },
  });
  const databases: DatabaseRow[] = dbResult.data?.trexDatabases || [];
  const allSchemas: SchemaRow[] = schemaResult.data?.trexSchemas || [];
  const tables: TableRow[] = tableResult.data?.trexTables || [];

  const schemas = selectedDatabase
    ? allSchemas.filter((s) => s.databaseName === selectedDatabase)
    : allSchemas;

  function refetchAll() {
    reexecuteDbs({ requestPolicy: "network-only" });
    reexecuteSchemas({ requestPolicy: "network-only" });
    reexecuteTables({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetchAll, 30000);
    return () => clearInterval(interval);
  }, []);

  const databaseColumns: Column<DatabaseRow>[] = [
    {
      header: "Name",
      cell: (row) => <code className="text-xs font-mono">{row.databaseName}</code>,
    },
    {
      header: "Type",
      cell: (row) => <Badge variant="secondary">{row.type}</Badge>,
    },
    {
      header: "Path",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">{row.path || "-"}</span>
      ),
    },
    {
      header: "Internal",
      cell: (row) => (
        <Badge variant={row.internal ? "secondary" : "default"}>
          {row.internal ? "Yes" : "No"}
        </Badge>
      ),
    },
  ];

  const schemaColumns: Column<SchemaRow>[] = [
    {
      header: "Database",
      cell: (row) => <code className="text-xs font-mono">{row.databaseName}</code>,
    },
    {
      header: "Schema",
      cell: (row) => <span className="text-sm font-medium">{row.schemaName}</span>,
    },
    {
      header: "Internal",
      cell: (row) => (
        <Badge variant={row.internal ? "secondary" : "default"}>
          {row.internal ? "Yes" : "No"}
        </Badge>
      ),
    },
  ];

  const tableColumns: Column<TableRow>[] = [
    {
      header: "Database",
      cell: (row) => <code className="text-xs font-mono">{row.databaseName}</code>,
    },
    {
      header: "Schema",
      cell: (row) => <code className="text-xs font-mono">{row.schemaName}</code>,
    },
    {
      header: "Table",
      cell: (row) => <span className="text-sm font-medium">{row.tableName}</span>,
    },
    {
      header: "Columns",
      cell: (row) => <span className="text-sm">{row.columnCount}</span>,
    },
    {
      header: "Est. Size",
      cell: (row) => <span className="text-sm">{row.estimatedSize}</span>,
    },
    {
      header: "PK",
      cell: (row) => (
        <Badge variant={row.hasPrimaryKey ? "default" : "secondary"}>
          {row.hasPrimaryKey ? "Yes" : "No"}
        </Badge>
      ),
    },
    {
      header: "Indexes",
      cell: (row) => <span className="text-sm">{row.indexCount}</span>,
    },
    {
      header: "Temp",
      cell: (row) =>
        row.temporary ? <Badge variant="secondary">Yes</Badge> : null,
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">TrexDB</h2>
          <p className="text-muted-foreground">
            {selectedDatabase
              ? `Browsing database: ${selectedDatabase}`
              : `${databases.length} database${databases.length === 1 ? "" : "s"} attached`}
          </p>
        </div>
        <div className="flex gap-2">
          {selectedDatabase && (
            <Button variant="outline" onClick={() => setSelectedDatabase(null)}>
              Clear Filter
            </Button>
          )}
          <Button variant="outline" onClick={refetchAll}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Databases</h3>
        <DataTable
          columns={databaseColumns}
          data={databases}
          loading={dbResult.fetching}
          onRowClick={(row) =>
            setSelectedDatabase(
              row.databaseName === selectedDatabase ? null : row.databaseName
            )
          }
          emptyMessage="No databases found."
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Schemas</h3>
        <DataTable
          columns={schemaColumns}
          data={schemas}
          loading={schemaResult.fetching}
          emptyMessage="No schemas found."
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Tables</h3>
        <DataTable
          columns={tableColumns}
          data={tables}
          loading={tableResult.fetching}
          emptyMessage="No tables found."
        />
      </div>
    </div>
  );
}

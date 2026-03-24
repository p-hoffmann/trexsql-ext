import { ChevronRight, ChevronDown, File, Folder, FolderOpen } from "lucide-react";
import type { FileTreeEntry } from "@/lib/types";

interface FileTreeProps {
  entries: FileTreeEntry[];
  selectedFile: string | null;
  expanded: Set<string>;
  onSelectFile: (path: string) => void;
  onToggleDir: (path: string) => void;
  depth?: number;
  searchQuery?: string;
}

function highlightMatch(text: string, query: string) {
  if (!query) return text;
  const lower = text.toLowerCase();
  const idx = lower.indexOf(query.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="bg-yellow-300/50 rounded-sm">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  );
}

export function FileTree({
  entries,
  selectedFile,
  expanded,
  onSelectFile,
  onToggleDir,
  depth = 0,
  searchQuery,
}: FileTreeProps) {
  const isSearching = !!searchQuery?.trim();

  return (
    <div>
      {entries.map((entry) => {
        const isExpanded = isSearching || expanded.has(entry.path);

        return (
          <div key={entry.path}>
            <button
              className={`flex items-center gap-1 w-full text-left px-2 py-0.5 text-xs hover:bg-muted/60 transition-colors ${
                selectedFile === entry.path ? "bg-primary/10 text-primary" : ""
              }`}
              style={{ paddingLeft: `${depth * 12 + 8}px` }}
              onClick={() => {
                if (entry.type === "directory") {
                  onToggleDir(entry.path);
                } else {
                  onSelectFile(entry.path);
                }
              }}
            >
              {entry.type === "directory" ? (
                <>
                  {isExpanded ? (
                    <ChevronDown className="h-3 w-3 shrink-0 opacity-50" />
                  ) : (
                    <ChevronRight className="h-3 w-3 shrink-0 opacity-50" />
                  )}
                  {isExpanded ? (
                    <FolderOpen className="h-3.5 w-3.5 shrink-0 text-blue-500" />
                  ) : (
                    <Folder className="h-3.5 w-3.5 shrink-0 text-blue-500" />
                  )}
                </>
              ) : (
                <>
                  <span className="w-3 shrink-0" />
                  <File className="h-3.5 w-3.5 shrink-0 opacity-50" />
                </>
              )}
              <span className="truncate">
                {searchQuery ? highlightMatch(entry.name, searchQuery) : entry.name}
              </span>
            </button>

            {entry.type === "directory" && isExpanded && entry.children && (
              <FileTree
                entries={entry.children}
                selectedFile={selectedFile}
                expanded={expanded}
                onSelectFile={onSelectFile}
                onToggleDir={onToggleDir}
                depth={depth + 1}
                searchQuery={searchQuery}
              />
            )}
          </div>
        );
      })}
    </div>
  );
}

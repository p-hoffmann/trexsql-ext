import { useState, useEffect, useMemo } from "react";
import { FileTree } from "./FileTree";
import { CodeViewer } from "./CodeViewer";
import { File, Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { filterFileTree } from "@/lib/fileTreeSearch";
import type { useFileTree } from "@/hooks/useFileTree";

interface CodeTabProps {
  fileTree: ReturnType<typeof useFileTree>;
}

export function CodeTab({ fileTree }: CodeTabProps) {
  const { tree, loading, selectedFile, fileContent, expanded, selectFile, toggleDir, saveFile } = fileTree;
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(searchQuery), 250);
    return () => clearTimeout(timer);
  }, [searchQuery]);

  const filteredTree = useMemo(
    () => filterFileTree(tree, debouncedQuery),
    [tree, debouncedQuery],
  );

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground text-sm">
        Loading files...
      </div>
    );
  }

  if (tree.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        <div className="text-center space-y-2">
          <File className="h-8 w-8 mx-auto opacity-30" />
          <p className="text-sm">No files in this app</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full">
      {/* File tree */}
      <div className="w-1/3 min-w-[160px] max-w-[280px] border-r overflow-auto py-1 flex flex-col">
        <div className="px-2 pb-1">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
            <Input
              placeholder="Search files..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="h-6 text-xs pl-6 pr-2"
            />
          </div>
        </div>
        <div className="flex-1 overflow-auto">
          <FileTree
            entries={filteredTree}
            selectedFile={selectedFile}
            expanded={expanded}
            onSelectFile={selectFile}
            onToggleDir={toggleDir}
            searchQuery={debouncedQuery}
          />
        </div>
      </div>

      {/* Code viewer */}
      <div className="flex-1 overflow-auto">
        {selectedFile && fileContent !== null ? (
          <CodeViewer
            content={fileContent}
            filePath={selectedFile}
            onSave={saveFile}
          />
        ) : (
          <div className="flex h-full items-center justify-center text-muted-foreground text-sm">
            Select a file to view
          </div>
        )}
      </div>
    </div>
  );
}

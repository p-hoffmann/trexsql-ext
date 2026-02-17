import type { ReactNode } from "react";

interface GridProps {
  cols?: string;
  children?: ReactNode;
}

export function Grid({ cols = "3", children }: GridProps) {
  return (
    <div
      className="grid gap-4"
      style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
    >
      {children}
    </div>
  );
}

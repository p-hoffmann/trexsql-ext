import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { ClipboardList } from "lucide-react";

interface PlanTabProps {
  content: string | null;
}

export function PlanTab({ content }: PlanTabProps) {
  if (!content) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        <div className="text-center space-y-2">
          <ClipboardList className="h-10 w-10 mx-auto opacity-30" />
          <p className="text-sm">No plan yet</p>
          <p className="text-xs text-muted-foreground">
            Switch to Plan mode and start a conversation to generate a plan.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto p-4">
      <div className="prose prose-sm dark:prose-invert max-w-none break-words text-sm">
        <ReactMarkdown
          remarkPlugins={[remarkGfm]}
          components={{
            pre({ children, ...props }) {
              return (
                <pre className="overflow-x-auto rounded-md bg-muted p-3 text-xs" {...props}>
                  {children}
                </pre>
              );
            },
            code({ children, className, ...props }) {
              const isInline = !className;
              if (isInline) {
                return (
                  <code className="rounded bg-muted px-1 py-0.5 text-xs" {...props}>
                    {children}
                  </code>
                );
              }
              return <code className={className} {...props}>{children}</code>;
            },
            table({ children, ...props }) {
              return (
                <div className="overflow-x-auto my-4">
                  <table className="min-w-full border-collapse text-xs" {...props}>
                    {children}
                  </table>
                </div>
              );
            },
            th({ children, ...props }) {
              return (
                <th className="border border-border bg-muted px-3 py-1.5 text-left font-medium" {...props}>
                  {children}
                </th>
              );
            },
            td({ children, ...props }) {
              return (
                <td className="border border-border px-3 py-1.5" {...props}>
                  {children}
                </td>
              );
            },
          }}
        >
          {content}
        </ReactMarkdown>
      </div>
    </div>
  );
}

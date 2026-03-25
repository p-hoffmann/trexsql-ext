// @ts-nocheck - Deno edge function
import type { ToolDefinition, AgentContext } from "./types.ts";

export const webFetchTool: ToolDefinition = {
  name: "web_fetch",
  description: "Fetch the content of a web page. Returns the text content with HTML tags stripped.",
  modifiesState: false,
  defaultConsent: "always",
  parameters: {
    type: "object",
    properties: {
      url: { type: "string", description: "The URL to fetch" },
      max_length: { type: "number", description: "Maximum characters to return (default 5000)" },
    },
    required: ["url"],
  },
  async execute(args: { url: string; max_length?: number }, ctx: AgentContext) {
    const maxLen = args.max_length || 5000;
    try {
      const res = await fetch(args.url, {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; DevX/1.0)" },
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) {
        return `Fetch failed: HTTP ${res.status}`;
      }
      const html = await res.text();

      function stripHtml(raw: string): string {
        // Loop to handle nested/malformed tags like <scr<script>ipt>
        let result = raw;
        let prev = "";
        while (result !== prev) {
          prev = result;
          result = result
            .replace(/<script[\s>][\s\S]*?<\/script\s*>/gi, "")
            .replace(/<style[\s>][\s\S]*?<\/style\s*>/gi, "");
        }
        return result.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
      }

      // Strip HTML tags and normalize whitespace
      const text = stripHtml(html);

      if (text.length > maxLen) {
        return text.substring(0, maxLen) + `\n\n[truncated — ${text.length - maxLen} chars omitted]`;
      }
      return text;
    } catch (err) {
      return `Fetch failed: ${err.message}`;
    }
  },
};

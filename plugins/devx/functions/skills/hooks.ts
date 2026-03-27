// @ts-nocheck - Deno edge function
/**
 * Hook dispatcher for the DevX agent pipeline.
 * Handles PreToolUse, PostToolUse, and Stop hooks.
 */

import type { Hook, HookResult } from "./types.ts";

type SqlFn = (query: string, params?: unknown[]) => Promise<{ rows: any[] }>;

/**
 * Load all enabled hooks for a given event type, ordered by sort_order.
 */
export async function loadHooks(
  userId: string,
  event: string,
  sqlFn: SqlFn,
): Promise<Hook[]> {
  const result = await sqlFn(
    `SELECT * FROM devx.hooks
     WHERE event = $1 AND enabled = true
       AND (user_id = $2 OR (is_builtin = true AND user_id IS NULL))
     ORDER BY sort_order ASC`,
    [event, userId],
  );
  return result.rows;
}

/**
 * Run PreToolUse hooks for a specific tool call.
 * Returns whether the tool call is allowed and any modifications to args.
 */
export async function runPreToolHooks(
  toolName: string,
  toolArgs: Record<string, unknown>,
  hooks: Hook[],
): Promise<{ allow: boolean; modifiedArgs?: Record<string, unknown> }> {
  const matchingHooks = hooks.filter((h) => matchesToolName(h.matcher, toolName));

  if (matchingHooks.length === 0) {
    return { allow: true };
  }

  let currentArgs = { ...toolArgs };

  for (const hook of matchingHooks) {
    try {
      const result = await executeHook(hook, {
        event: "PreToolUse",
        toolName,
        toolArgs: currentArgs,
      });

      if (result.action === "deny") {
        return { allow: false };
      }

      if (result.action === "modify" && result.modifications) {
        currentArgs = { ...currentArgs, ...result.modifications };
      }
    } catch (err) {
      console.error(`[hooks] PreToolUse hook error:`, err);
      // Hook errors don't block tool execution by default
    }
  }

  const argsChanged = JSON.stringify(currentArgs) !== JSON.stringify(toolArgs);
  return { allow: true, modifiedArgs: argsChanged ? currentArgs : undefined };
}

/**
 * Run PostToolUse hooks after a tool has executed.
 * Can modify the tool result string.
 */
export async function runPostToolHooks(
  toolName: string,
  toolArgs: Record<string, unknown>,
  toolResult: string,
  hooks: Hook[],
): Promise<string> {
  const matchingHooks = hooks.filter((h) => matchesToolName(h.matcher, toolName));

  if (matchingHooks.length === 0) {
    return toolResult;
  }

  let currentResult = toolResult;

  for (const hook of matchingHooks) {
    try {
      const result = await executeHook(hook, {
        event: "PostToolUse",
        toolName,
        toolArgs,
        toolResult: currentResult,
      });

      if (result.modifiedResult) {
        currentResult = result.modifiedResult;
      }
    } catch (err) {
      console.error(`[hooks] PostToolUse hook error:`, err);
    }
  }

  return currentResult;
}

/**
 * Run Stop hooks when the agent loop finishes.
 */
export async function runStopHooks(
  hooks: Hook[],
  context: { chatId: string; content: string },
): Promise<void> {
  for (const hook of hooks) {
    try {
      await executeHook(hook, {
        event: "Stop",
        chatId: context.chatId,
        content: context.content.slice(0, 5000), // Limit context size
      });
    } catch (err) {
      console.error(`[hooks] Stop hook error:`, err);
    }
  }
}

// --- Hook execution ---

async function executeHook(
  hook: Hook,
  input: Record<string, unknown>,
): Promise<HookResult> {
  if (hook.hook_type === "command") {
    return executeCommandHook(hook, input);
  }

  if (hook.hook_type === "prompt") {
    // Prompt hooks return the prompt text for the caller to handle.
    // In the agent pipeline, this would be injected as a system message.
    return { action: "approve" };
  }

  return { action: "approve" };
}

// Executables allowed for hook commands
const ALLOWED_EXECUTABLES = new Set([
  "node", "deno", "python", "python3", "bash", "sh", "bun", "npx", "uvx",
]);

async function executeCommandHook(
  hook: Hook,
  input: Record<string, unknown>,
): Promise<HookResult> {
  if (!hook.command) return { action: "approve" };

  const timeout = hook.timeout_ms || 10000;

  try {
    // Parse command into executable + args
    const parts = hook.command.split(/\s+/);
    const executable = parts[0];

    // Validate executable against allow-list
    if (!ALLOWED_EXECUTABLES.has(executable)) {
      console.error(`[hooks] Blocked disallowed executable: ${executable}`);
      return { action: "approve" };
    }

    const cmd = new Deno.Command(executable, {
      args: parts.slice(1),
      stdin: "piped",
      stdout: "piped",
      stderr: "piped",
      env: {
        DEVX_HOOK_EVENT: String(input.event || ""),
        DEVX_TOOL_NAME: String(input.toolName || ""),
      },
    });

    const process = cmd.spawn();

    // Write input as JSON to stdin
    const writer = process.stdin.getWriter();
    await writer.write(new TextEncoder().encode(JSON.stringify(input)));
    await writer.close();

    // Wait for completion with timeout
    const result = await Promise.race([
      process.output(),
      new Promise<never>((_, reject) =>
        setTimeout(() => {
          try { process.kill("SIGTERM"); } catch { /* already dead */ }
          reject(new Error(`Hook timed out after ${timeout}ms`));
        }, timeout),
      ),
    ]);

    if (!result.success) {
      const stderr = new TextDecoder().decode(result.stderr);
      console.error(`[hooks] Command hook failed:`, stderr);
      return { action: "approve" }; // Don't block on hook failures
    }

    const stdout = new TextDecoder().decode(result.stdout).trim();
    if (!stdout) return { action: "approve" };

    // Parse JSON response from hook
    try {
      const parsed = JSON.parse(stdout);
      return {
        action: parsed.action || "approve",
        modifications: parsed.modifications,
        modifiedResult: parsed.modifiedResult,
      };
    } catch {
      // If output is just "deny" or "approve" as plain text
      if (stdout === "deny") return { action: "deny" };
      return { action: "approve" };
    }
  } catch (err) {
    console.error(`[hooks] Command hook execution error:`, err);
    return { action: "approve" };
  }
}

// --- Helpers ---

/**
 * Check if a tool name matches a hook's matcher pattern.
 * Matcher is a pipe-separated list of tool name patterns, or null (matches all).
 */
function matchesToolName(matcher: string | null, toolName: string): boolean {
  // No matcher = matches all tools
  if (!matcher || matcher === "*") return true;

  const patterns = matcher.split("|").map((p) => p.trim());

  for (const pattern of patterns) {
    // Exact match
    if (pattern === toolName) return true;

    // Simple glob: "write_*" matches "write_file"
    if (pattern.endsWith("*")) {
      const prefix = pattern.slice(0, -1);
      if (toolName.startsWith(prefix)) return true;
    }

    // Case-insensitive exact match
    if (pattern.toLowerCase() === toolName.toLowerCase()) return true;
  }

  return false;
}

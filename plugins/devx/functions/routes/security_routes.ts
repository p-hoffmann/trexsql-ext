// @ts-nocheck - Deno edge function
import { getAppWorkspacePath } from "../tools/workspace.ts";
import { duckdb, escapeSql } from "../duckdb.ts";
import { SECURITY_REVIEW_SYSTEM_PROMPT, parseSecurityFindings } from "../security_review_prompt.ts";
import { CODE_REVIEW_SYSTEM_PROMPT, parseCodeReviewFindings } from "../code_review_prompt.ts";

const EXCLUDED_DIRS = new Set([
  "node_modules", ".git", "dist", "build", ".next", ".venv", "venv",
  "__pycache__", ".cache", ".turbo", ".nuxt", "coverage",
]);

const CODE_EXTENSIONS = /\.(ts|tsx|js|jsx|mjs|cjs|json|env|yaml|yml|py|sql|html|css|vue|svelte)$/;

const ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages";
const OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions";
const GOOGLE_GENERATE_URL = "https://generativelanguage.googleapis.com/v1beta/models";

export async function handleSecurityRoutes(path, method, req, userId, sql, corsHeaders) {
  // POST /apps/:id/security/scan — fast npm audit + secret scan (unchanged)
  const scanMatch = path.match(/\/apps\/([^/]+)\/security\/scan$/);
  if (scanMatch && method === "POST") {
    const appId = scanMatch[1];
    const appCheck = await sql(`SELECT id FROM devx.apps WHERE id = $1 AND user_id = $2`, [appId, userId]);
    if (appCheck.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }

    const wsPath = getAppWorkspacePath(userId, appId);
    const findings = [];

    // Run npm audit
    try {
      const result = JSON.parse(await duckdb(
        `SELECT * FROM trex_devx_run_command('${escapeSql(wsPath)}', 'npm audit --json')`
      ));
      const output = result.output || "";
      try {
        const audit = JSON.parse(output);
        if (audit.vulnerabilities) {
          for (const [name, vuln] of Object.entries(audit.vulnerabilities)) {
            findings.push({
              severity: vuln.severity || "moderate",
              title: `Vulnerable dependency: ${name}`,
              description: `${vuln.via?.[0]?.title || vuln.via?.[0] || "Known vulnerability"}. Fix: ${vuln.fixAvailable ? "Update available" : "No fix available"}`,
            });
          }
        }
      } catch { /* not valid JSON */ }
    } catch { /* npm audit not available */ }

    // Basic secret scanning
    const secretPatterns = [
      { pattern: /(?:api[_-]?key|apikey)\s*[:=]\s*["'][a-zA-Z0-9]{20,}["']/i, title: "Hardcoded API key" },
      { pattern: /(?:password|passwd|pwd)\s*[:=]\s*["'][^"']{4,}["']/i, title: "Hardcoded password" },
      { pattern: /(?:secret|token)\s*[:=]\s*["'][a-zA-Z0-9]{20,}["']/i, title: "Hardcoded secret/token" },
      { pattern: /-----BEGIN (?:RSA |EC )?PRIVATE KEY-----/, title: "Private key in source" },
    ];

    async function scanDir(dir, depth = 0) {
      if (depth > 3) return;
      try {
        for await (const entry of Deno.readDir(dir)) {
          if (entry.name.startsWith(".") || entry.name === "node_modules") continue;
          const fullPath = `${dir}/${entry.name}`;
          if (entry.isDirectory) {
            await scanDir(fullPath, depth + 1);
          } else if (entry.isFile && /\.(ts|tsx|js|jsx|json|env|yaml|yml)$/.test(entry.name)) {
            try {
              const content = await Deno.readTextFile(fullPath);
              const relPath = fullPath.replace(wsPath + "/", "");
              for (const sp of secretPatterns) {
                if (sp.pattern.test(content)) {
                  findings.push({
                    severity: "high",
                    title: sp.title,
                    description: `Potential ${sp.title.toLowerCase()} found`,
                    file: relPath,
                  });
                }
              }
            } catch { /* skip unreadable files */ }
          }
        }
      } catch { /* skip unreadable dirs */ }
    }

    await scanDir(wsPath);

    return Response.json({ findings }, { headers: corsHeaders });
  }

  // ── AI-powered review (shared logic) ──────────────────────────────

  // Helper: run an AI review and return SSE Response
  async function runAIReview(opts: {
    appId: string;
    systemPrompt: string;
    userMessagePrefix: string;
    parseFindings: (text: string) => { title: string; level: string; description: string }[];
    table: string;
    eventPrefix: string;
  }) {
    const settingsResult = await sql(
      `SELECT provider, model, api_key, base_url FROM devx.settings WHERE user_id = $1 LIMIT 1`,
      [userId],
    );
    if (settingsResult.rows.length === 0 || !settingsResult.rows[0].api_key) {
      return Response.json(
        { error: "AI provider not configured. Set your API key in Settings." },
        { status: 400, headers: corsHeaders },
      );
    }

    const settings = settingsResult.rows[0];
    const wsPath = getAppWorkspacePath(userId, opts.appId);
    const files = await collectCodeFiles(wsPath);

    if (files.length === 0) {
      return Response.json(
        { error: "No code files found to review" },
        { status: 400, headers: corsHeaders },
      );
    }

    // Fetch previous review to include as reference
    let previousContext = "";
    try {
      const prevResult = await sql(
        `SELECT findings, created_at FROM devx.${opts.table} WHERE app_id = $1 AND user_id = $2 ORDER BY created_at DESC LIMIT 1`,
        [opts.appId, userId],
      );
      if (prevResult.rows.length > 0) {
        const prevFindings = typeof prevResult.rows[0].findings === "string"
          ? JSON.parse(prevResult.rows[0].findings)
          : prevResult.rows[0].findings;
        if (prevFindings && prevFindings.length > 0) {
          const prevList = prevFindings.map((f: any) =>
            `- [${f.level}] ${f.title}: ${f.description.substring(0, 200)}`
          ).join("\n");
          previousContext = `\n\n---\n\nIMPORTANT: A previous review found these issues (from ${prevResult.rows[0].created_at}):\n\n${prevList}\n\nFor each previous finding, check if it is still present in the code. If it is still present, include it again in your findings. If it has been fixed, do NOT include it. Do NOT drop previous findings just because you want to report new ones — if the vulnerability still exists in the code, it MUST appear in your output. New findings should also be reported.`;
        }
      }
    } catch { /* ignore — first review */ }

    const codeContext = files.map((f) => `--- ${f.path} ---\n${f.content}`).join("\n\n");
    const userMessage = `${opts.userMessagePrefix}:\n\n${codeContext}${previousContext}`;

    const stream = new ReadableStream({
      async start(controller) {
        const send = (data: any) => {
          controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`));
        };

        try {
          send({ type: `${opts.eventPrefix}_progress`, message: `Reviewing ${files.length} files...` });

          const fullResponse = await callAIProvider(settings, opts.systemPrompt, userMessage, send, opts.eventPrefix);
          const findings = opts.parseFindings(fullResponse);

          const insertResult = await sql(
            `INSERT INTO devx.${opts.table} (app_id, user_id, findings)
             VALUES ($1, $2, $3) RETURNING id, created_at`,
            [opts.appId, userId, JSON.stringify(findings)],
          );

          send({
            type: `${opts.eventPrefix}_done`,
            review: { id: insertResult.rows[0].id, findings, created_at: insertResult.rows[0].created_at },
          });
        } catch (err) {
          send({ type: `${opts.eventPrefix}_error`, error: err.message });
        }

        controller.close();
      },
    });

    return new Response(stream, {
      headers: { ...corsHeaders, "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" },
    });
  }

  // POST /apps/:id/security/review — AI-powered security review
  const secReviewMatch = path.match(/\/apps\/([^/]+)\/security\/review$/);
  if (secReviewMatch && method === "POST") {
    const appId = secReviewMatch[1];
    const appCheck = await sql(`SELECT id FROM devx.apps WHERE id = $1 AND user_id = $2`, [appId, userId]);
    if (appCheck.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }
    return runAIReview({
      appId,
      systemPrompt: SECURITY_REVIEW_SYSTEM_PROMPT,
      userMessagePrefix: "Review the following codebase for security vulnerabilities",
      parseFindings: parseSecurityFindings,
      table: "security_reviews",
      eventPrefix: "review",
    });
  }

  // GET /apps/:id/security/reviews — get latest security review
  const secReviewsMatch = path.match(/\/apps\/([^/]+)\/security\/reviews$/);
  if (secReviewsMatch && method === "GET") {
    const appId = secReviewsMatch[1];
    const appCheck = await sql(`SELECT id FROM devx.apps WHERE id = $1 AND user_id = $2`, [appId, userId]);
    if (appCheck.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }
    const result = await sql(
      `SELECT id, findings, created_at FROM devx.security_reviews WHERE app_id = $1 AND user_id = $2 ORDER BY created_at DESC LIMIT 1`,
      [appId, userId],
    );
    return Response.json(result.rows.length === 0 ? null : result.rows[0], { headers: corsHeaders });
  }

  // POST /apps/:id/code/review — AI-powered code review
  const codeReviewMatch = path.match(/\/apps\/([^/]+)\/code\/review$/);
  if (codeReviewMatch && method === "POST") {
    const appId = codeReviewMatch[1];
    const appCheck = await sql(`SELECT id FROM devx.apps WHERE id = $1 AND user_id = $2`, [appId, userId]);
    if (appCheck.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }
    return runAIReview({
      appId,
      systemPrompt: CODE_REVIEW_SYSTEM_PROMPT,
      userMessagePrefix: "Review the following codebase for bugs, logic errors, and code quality issues",
      parseFindings: parseCodeReviewFindings,
      table: "code_reviews",
      eventPrefix: "code_review",
    });
  }

  // GET /apps/:id/code/reviews — get latest code review
  const codeReviewsMatch = path.match(/\/apps\/([^/]+)\/code\/reviews$/);
  if (codeReviewsMatch && method === "GET") {
    const appId = codeReviewsMatch[1];
    const appCheck = await sql(`SELECT id FROM devx.apps WHERE id = $1 AND user_id = $2`, [appId, userId]);
    if (appCheck.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }
    const result = await sql(
      `SELECT id, findings, created_at FROM devx.code_reviews WHERE app_id = $1 AND user_id = $2 ORDER BY created_at DESC LIMIT 1`,
      [appId, userId],
    );
    return Response.json(result.rows.length === 0 ? null : result.rows[0], { headers: corsHeaders });
  }

  return null;
}

// ── Shared Helpers ────────────────────────────────────────────────

const MAX_CONTEXT_SIZE = 200_000;

async function collectCodeFiles(wsPath: string): Promise<{ path: string; content: string }[]> {
  const files: { path: string; content: string }[] = [];
  let totalSize = 0;

  async function walk(dir: string, depth = 0) {
    if (depth > 5 || totalSize > MAX_CONTEXT_SIZE) return;
    try {
      for await (const entry of Deno.readDir(dir)) {
        if (totalSize > MAX_CONTEXT_SIZE) return;
        if (entry.name.startsWith(".")) continue;
        if (entry.isDirectory && EXCLUDED_DIRS.has(entry.name)) continue;

        const fullPath = `${dir}/${entry.name}`;
        if (entry.isDirectory) {
          await walk(fullPath, depth + 1);
        } else if (entry.isFile && CODE_EXTENSIONS.test(entry.name)) {
          try {
            const content = await Deno.readTextFile(fullPath);
            if (content.length > 50_000) continue;
            const relPath = fullPath.replace(wsPath + "/", "");
            files.push({ path: relPath, content });
            totalSize += content.length;
          } catch { /* skip unreadable */ }
        }
      }
    } catch { /* skip unreadable dirs */ }
  }

  await walk(wsPath);
  return files;
}

// ── AI Provider Calls ─────────────────────────────────────────────

async function callAIProvider(
  settings: { provider: string; model: string; api_key: string; base_url?: string },
  systemPrompt: string,
  userMessage: string,
  send: (data: any) => void,
  eventPrefix: string,
): Promise<string> {
  if (settings.provider === "anthropic") {
    return callAnthropic(settings, systemPrompt, userMessage, send, eventPrefix);
  } else if (settings.provider === "google") {
    return callGoogle(settings, systemPrompt, userMessage, send, eventPrefix);
  } else if (settings.provider === "bedrock") {
    return callBedrock(settings, systemPrompt, userMessage, send, eventPrefix);
  } else {
    return callOpenAI(settings, systemPrompt, userMessage, send, eventPrefix);
  }
}

async function callAnthropic(
  settings: { model: string; api_key: string },
  systemPrompt: string,
  userMessage: string,
  send: (data: any) => void,
  eventPrefix: string,
): Promise<string> {
  const response = await fetch(ANTHROPIC_MESSAGES_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": settings.api_key,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: settings.model,
      max_tokens: 8192,
      stream: true,
      system: systemPrompt,
      messages: [{ role: "user", content: userMessage }],
    }),
  });

  if (!response.ok) {
    const errBody = await response.text();
    throw new Error(`Anthropic API error ${response.status}: ${errBody}`);
  }

  return readSSEStream(response, () => {
    send({ type: `${eventPrefix}_progress`, message: "Analyzing code..." });
  }, "anthropic");
}

async function callOpenAI(
  settings: { model: string; api_key: string; base_url?: string },
  systemPrompt: string,
  userMessage: string,
  send: (data: any) => void,
  eventPrefix: string,
): Promise<string> {
  const baseUrl = settings.base_url
    ? `${settings.base_url}/v1/chat/completions`
    : OPENAI_CHAT_URL;

  const response = await fetch(baseUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${settings.api_key}`,
    },
    body: JSON.stringify({
      model: settings.model,
      stream: true,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userMessage },
      ],
    }),
  });

  if (!response.ok) {
    const errBody = await response.text();
    throw new Error(`OpenAI API error ${response.status}: ${errBody}`);
  }

  return readSSEStream(response, () => {
    send({ type: `${eventPrefix}_progress`, message: "Analyzing code..." });
  }, "openai");
}

async function callGoogle(
  settings: { model: string; api_key: string },
  systemPrompt: string,
  userMessage: string,
  send: (data: any) => void,
  eventPrefix: string,
): Promise<string> {
  const googleUrl = `${GOOGLE_GENERATE_URL}/${settings.model}:streamGenerateContent?alt=sse&key=${settings.api_key}`;

  const response = await fetch(googleUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: systemPrompt }] },
      contents: [{ role: "user", parts: [{ text: userMessage }] }],
    }),
  });

  if (!response.ok) {
    const errBody = await response.text();
    throw new Error(`Google API error ${response.status}: ${errBody}`);
  }

  return readSSEStream(response, () => {
    send({ type: `${eventPrefix}_progress`, message: "Analyzing code..." });
  }, "google");
}

async function callBedrock(
  settings: { model: string; api_key: string; base_url?: string },
  systemPrompt: string,
  userMessage: string,
  send: (data: any) => void,
  eventPrefix: string,
): Promise<string> {
  const region = settings.base_url || "us-east-1";

  // Extract bearer token from api_key (JSON credentials)
  let bearerToken = "";
  try {
    const creds = JSON.parse(settings.api_key);
    bearerToken = creds.bearerToken || "";
  } catch { /* not JSON */ }
  if (!bearerToken) {
    bearerToken = Deno.env.get("AWS_BEARER_TOKEN_BEDROCK") || "";
  }
  if (!bearerToken) {
    throw new Error("AWS Bearer Token not configured for Bedrock");
  }

  const host = `bedrock-runtime.${region}.amazonaws.com`;
  const url = `https://${host}/model/${settings.model}/converse-stream`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${bearerToken}`,
    },
    body: JSON.stringify({
      system: [{ text: systemPrompt }],
      messages: [{ role: "user", content: [{ text: userMessage }] }],
      inferenceConfig: { maxTokens: 8192 },
    }),
  });

  if (!response.ok) {
    const errBody = await response.text();
    throw new Error(`Bedrock API error ${response.status}: ${errBody}`);
  }

  // Parse AWS event stream binary protocol
  let fullContent = "";
  const reader = response.body!.getReader();
  let buf = new Uint8Array(0);

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const newBuf = new Uint8Array(buf.length + value.length);
    newBuf.set(buf);
    newBuf.set(value, buf.length);
    buf = newBuf;

    while (buf.length >= 12) {
      const view = new DataView(buf.buffer, buf.byteOffset);
      const totalLen = view.getUint32(0);
      if (buf.length < totalLen) break;

      const headersLen = view.getUint32(4);
      const payloadOffset = 12 + headersLen;
      const payloadLen = totalLen - payloadOffset - 4;

      if (payloadLen > 0) {
        try {
          const payloadStr = new TextDecoder().decode(buf.slice(payloadOffset, payloadOffset + payloadLen));
          const payload = JSON.parse(payloadStr);
          const text = payload.delta?.text ?? payload.contentBlockDelta?.delta?.text;
          if (text) {
            fullContent += text;
            send({ type: `${eventPrefix}_progress`, message: "Analyzing code..." });
          }
        } catch { /* skip */ }
      }
      buf = buf.slice(totalLen);
    }
  }

  return fullContent;
}

/**
 * Read an SSE stream and extract text content from provider-specific format.
 */
async function readSSEStream(
  response: Response,
  onChunk: (text: string) => void,
  provider: "anthropic" | "openai" | "google",
): Promise<string> {
  let fullContent = "";
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      const data = line.slice(6).trim();
      if (data === "[DONE]") continue;

      try {
        const parsed = JSON.parse(data);
        let text = "";

        if (provider === "anthropic") {
          if (parsed.type === "content_block_delta" && parsed.delta?.text) {
            text = parsed.delta.text;
          }
        } else if (provider === "openai") {
          text = parsed.choices?.[0]?.delta?.content || "";
        } else if (provider === "google") {
          text = parsed.candidates?.[0]?.content?.parts?.[0]?.text || "";
        }

        if (text) {
          fullContent += text;
          onChunk(text);
        }
      } catch { /* skip malformed */ }
    }
  }

  return fullContent;
}

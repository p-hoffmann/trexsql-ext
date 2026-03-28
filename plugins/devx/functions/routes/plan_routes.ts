// @ts-nocheck - Deno edge function
import { resolveQuestionnaire } from "../tools/plan_tools.ts";

export async function handlePlanRoutes(path, method, req, userId, sql, corsHeaders) {
  // GET /chats/:id/plan — fetch current plan
  const planGetMatch = path.match(/\/chats\/([^/]+)\/plan$/);
  if (planGetMatch && method === "GET") {
    const chatId = planGetMatch[1];
    const chatCheck = await sql(`SELECT id FROM devx.chats WHERE id = $1 AND user_id = $2`, [chatId, userId]);
    if (chatCheck.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }
    const result = await sql(
      `SELECT id, chat_id, content, status, created_at, updated_at FROM devx.plans WHERE chat_id = $1`,
      [chatId],
    );
    if (result.rows.length === 0) {
      return Response.json(null, { headers: corsHeaders });
    }
    return Response.json(result.rows[0], { headers: corsHeaders });
  }

  // POST /chats/:id/plan/answer — resolve pending questionnaire via DB
  const answerMatch = path.match(/\/chats\/([^/]+)\/plan\/answer$/);
  if (answerMatch && method === "POST") {
    const body = await req.json();
    const { requestId, answers } = body;
    if (!requestId || !answers) {
      return Response.json({ error: "requestId and answers required" }, { status: 400, headers: corsHeaders });
    }
    const resolved = await resolveQuestionnaire(requestId, answers, userId, sql);
    if (!resolved) {
      return Response.json({ error: "Questionnaire not found or expired" }, { status: 404, headers: corsHeaders });
    }
    return Response.json({ ok: true }, { headers: corsHeaders });
  }

  // GET /apps/:id/plans — list all plans for an app (via chats)
  const appPlansMatch = path.match(/\/apps\/([^/]+)\/plans$/);
  if (appPlansMatch && method === "GET") {
    const appId = appPlansMatch[1];
    const result = await sql(
      `SELECT p.id, p.chat_id, p.content, p.status, p.created_at, p.updated_at, c.title as chat_title
       FROM devx.plans p
       JOIN devx.chats c ON c.id = p.chat_id
       WHERE c.app_id = $1 AND c.user_id = $2
       ORDER BY p.updated_at DESC`,
      [appId, userId],
    );
    return Response.json(result.rows, { headers: corsHeaders });
  }

  // PATCH /plans/:id/status — update plan status
  const statusMatch = path.match(/\/plans\/([^/]+)\/status$/);
  if (statusMatch && method === "PATCH") {
    const planId = statusMatch[1];
    const body = await req.json();
    const { status } = body;
    if (!status || !["draft", "accepted", "rejected", "implemented"].includes(status)) {
      return Response.json({ error: "Invalid status" }, { status: 400, headers: corsHeaders });
    }
    const result = await sql(
      `UPDATE devx.plans SET status = $1, updated_at = NOW()
       WHERE id = $2 AND chat_id IN (SELECT id FROM devx.chats WHERE user_id = $3)
       RETURNING id, status`,
      [status, planId, userId],
    );
    if (result.rows.length === 0) {
      return Response.json({ error: "Not found" }, { status: 404, headers: corsHeaders });
    }
    return Response.json(result.rows[0], { headers: corsHeaders });
  }

  return null;
}

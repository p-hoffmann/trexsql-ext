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

  // POST /chats/:id/plan/answer — resolve pending questionnaire
  const answerMatch = path.match(/\/chats\/([^/]+)\/plan\/answer$/);
  if (answerMatch && method === "POST") {
    const body = await req.json();
    const { requestId, answers } = body;
    if (!requestId || !answers) {
      return Response.json({ error: "requestId and answers required" }, { status: 400, headers: corsHeaders });
    }
    const resolved = resolveQuestionnaire(requestId, answers, userId);
    if (!resolved) {
      return Response.json({ error: "Questionnaire not found or expired" }, { status: 404, headers: corsHeaders });
    }
    return Response.json({ ok: true }, { headers: corsHeaders });
  }

  return null;
}

// @ts-nocheck - Deno edge function
/**
 * Plan mode tools: questionnaire, write_plan, exit_plan.
 * Uses the same blocking-resolution pattern as the consent system.
 */
import type { ToolDefinition } from "./types.ts";

// In-memory map for pending questionnaire responses
const pendingQuestionnaires = new Map();

/** Resolve a pending questionnaire (called from plan_routes) */
export function resolveQuestionnaire(requestId, answers, userId) {
  const entry = pendingQuestionnaires.get(requestId);
  if (entry) {
    if (entry.userId && entry.userId !== userId) return false;
    entry.resolve(answers);
    pendingQuestionnaires.delete(requestId);
    return true;
  }
  return false;
}

/** Clean up questionnaires for a chat (called on stream abort) */
export function clearPendingQuestionnaires(chatId) {
  for (const [requestId, entry] of pendingQuestionnaires.entries()) {
    if (entry.chatId === chatId) {
      entry.resolve(null);
      pendingQuestionnaires.delete(requestId);
    }
  }
}

export const planningQuestionnaireTool: ToolDefinition<{
  questions: Array<{
    id: string;
    type: "text" | "radio" | "checkbox";
    label: string;
    options?: string[];
  }>;
}> = {
  name: "planning_questionnaire",
  description:
    "Ask the user clarifying questions during planning. Supports text input, radio (single choice), and checkbox (multiple choice) question types. Blocks until the user answers.",
  parameters: {
    type: "object",
    properties: {
      questions: {
        type: "array",
        items: {
          type: "object",
          properties: {
            id: { type: "string", description: "Unique identifier for this question" },
            type: { type: "string", enum: ["text", "radio", "checkbox"], description: "Question type" },
            label: { type: "string", description: "The question text" },
            options: {
              type: "array",
              items: { type: "string" },
              description: "Options for radio/checkbox questions",
            },
          },
          required: ["id", "type", "label"],
        },
        description: "Questions to ask the user",
      },
    },
    required: ["questions"],
  },
  defaultConsent: "always",
  modifiesState: false,

  async execute(args, ctx) {
    const requestId = crypto.randomUUID();

    // Send questionnaire to client
    ctx.send({ type: "questionnaire", requestId, questions: args.questions });

    // Block until user answers
    const answers = await new Promise((resolve) => {
      pendingQuestionnaires.set(requestId, {
        resolve,
        userId: ctx.userId,
        chatId: ctx.chatId,
      });
      // Timeout after 10 minutes
      setTimeout(() => {
        if (pendingQuestionnaires.has(requestId)) {
          pendingQuestionnaires.delete(requestId);
          resolve(null);
        }
      }, 10 * 60 * 1000);
    });

    if (!answers) {
      return "User did not respond to the questionnaire.";
    }

    return `User answers:\n${JSON.stringify(answers, null, 2)}`;
  },
};

export const writePlanTool: ToolDefinition<{ content: string }> = {
  name: "write_plan",
  description:
    "Write or update the implementation plan for this conversation. The plan content should be markdown. This stores the plan and displays it to the user in the Plan tab.",
  parameters: {
    type: "object",
    properties: {
      content: { type: "string", description: "Plan content in markdown format" },
    },
    required: ["content"],
  },
  defaultConsent: "always",
  modifiesState: true,

  async execute(args, ctx) {
    // Upsert plan for this chat
    await ctx.sql(
      `INSERT INTO devx.plans (chat_id, content, status)
       VALUES ($1, $2, 'draft')
       ON CONFLICT (chat_id) DO UPDATE SET content = EXCLUDED.content, updated_at = NOW()`,
      [ctx.chatId, args.content],
    );

    // Notify client
    ctx.send({ type: "plan_update", content: args.content });

    return "Plan written and displayed to user.";
  },
};

export const exitPlanTool: ToolDefinition<{ confirmation: boolean }> = {
  name: "exit_plan",
  description:
    "Exit plan mode and switch to implementation. Call this when the plan is finalized and the user has accepted it. The chat mode will switch from 'plan' to 'agent'.",
  parameters: {
    type: "object",
    properties: {
      confirmation: {
        type: "boolean",
        description: "Set to true to confirm exiting plan mode",
      },
    },
    required: ["confirmation"],
  },
  defaultConsent: "always",
  modifiesState: true,

  async execute(args, ctx) {
    if (!args.confirmation) {
      return "Plan mode exit cancelled.";
    }

    // Update plan status
    await ctx.sql(
      `UPDATE devx.plans SET status = 'accepted', updated_at = NOW() WHERE chat_id = $1`,
      [ctx.chatId],
    );

    // Switch chat mode to agent
    await ctx.sql(
      `UPDATE devx.chats SET mode = 'agent', updated_at = NOW() WHERE id = $1`,
      [ctx.chatId],
    );

    ctx.send({ type: "plan_exit", mode: "agent" });

    return "Plan accepted. Chat mode switched to agent for implementation.";
  },
};

---
name: checkpoint
version: 1.0.0
description: |
  Save and resume working state checkpoints. Captures git state, decisions made,
  and remaining work so you can pick up exactly where you left off — even across
  Conductor workspace handoffs between branches.
  Use when asked to "checkpoint", "save progress", "where was I", "resume",
  "what was I working on", or "pick up where I left off".
  Proactively suggest when a session is ending, the user is switching context,
  or before a long break.
allowed-tools:
  - Read
  - Write
  - Edit
  - Bash
  - Grep
  - Glob
  - Agent
  - AskUserQuestion
  - WebSearch
aliases:
  - checkpoint
  - save-progress
  - resume
  - where-was-i
  - pick-up
---

# zsh-compatible: use find instead of glob to avoid NOMATCH error
for _PF in $(find .devx/analytics -maxdepth 1 -name '.pending-*' 2>/dev/null); do
  if [ -f "$_PF" ]; then
    if [ "$_TEL" != "off" ] && [ -x " ]; then
       --event-type skill_run --skill _pending_finalize --outcome unknown --session-id "$_SESSION_ID" 2>/dev/null || true
    fi
    rm -f "$_PF" 2>/dev/null || true
  fi
  break
done
# Learnings count

_LEARN_FILE="${DEVX_HOME:-$HOME/.DevX}/projects/${SLUG:-unknown}/learnings.jsonl"
if [ -f "$_LEARN_FILE" ]; then
  _LEARN_COUNT=$(wc -l < "$_LEARN_FILE" 2>/dev/null | tr -d ' ')
  echo "LEARNINGS: $_LEARN_COUNT entries loaded"
  if [ "$_LEARN_COUNT" -gt 5 ] 2>/dev/null; then
     --limit 3 2>/dev/null || true
  fi
else
  echo "LEARNINGS: 0"
fi
# Session timeline: record skill start (local-only, never sent anywhere)
 '{"skill":"checkpoint","event":"started","branch":"'"$_BRANCH"'","session":"'"$_SESSION_ID"'"}' 2>/dev/null &
# Check if CLAUDE.md has routing rules
_HAS_ROUTING="no"
if [ -f CLAUDE.md ] && grep -q "## Skill routing" CLAUDE.md 2>/dev/null; then
  _HAS_ROUTING="yes"
fi
_ROUTING_DECLINED=$( get routing_declined 2>/dev/null || echo "false")
echo "HAS_ROUTING: $_HAS_ROUTING"
echo "ROUTING_DECLINED: $_ROUTING_DECLINED"
```

If `HAS_ROUTING` is `no` AND `ROUTING_DECLINED` is `false` AND `PROACTIVE_PROMPTED` is `yes`:
Check if a CLAUDE.md file exists in the project root. If it does not exist, create it.

Use AskUserQuestion:

Options:
- A) Add routing rules to CLAUDE.md (recommended)
- B) No thanks, I'll invoke skills manually

If A: Append this section to the end of CLAUDE.md:

```markdown

# Session timeline: record skill completion (local-only, never sent anywhere)
 '{"skill":"SKILL_NAME","event":"completed","branch":"'$(git branch --show-current 2>/dev/null || echo unknown)'","outcome":"OUTCOME","duration_s":"'"$_TEL_DUR"'","session":"'"$_SESSION_ID"'"}' 2>/dev/null || true
# Local analytics (gated on telemetry setting)
if [ "$_TEL" != "off" ]; then
echo '{"skill":"SKILL_NAME","duration_s":"'"$_TEL_DUR"'","outcome":"OUTCOME","browse":"USED_BROWSE","session":"'"$_SESSION_ID"'","ts":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' >> .devx/analytics/skill-usage.jsonl 2>/dev/null || true
fi
# Remote telemetry (opt-in, requires binary)
if [ "$_TEL" != "off" ] && [ -x  ]; then
   \
    --skill "SKILL_NAME" --duration "$_TEL_DUR" --outcome "OUTCOME" \
    --used-browse "USED_BROWSE" --session-id "$_SESSION_ID" 2>/dev/null &
fi
```

Replace `SKILL_NAME` with the actual skill name from frontmatter, `OUTCOME` with
success/error/abort, and `USED_BROWSE` with true/false based on whether `use BrowserNavigate, BrowserClick, BrowserScreenshot tools` was used.
If you cannot determine the outcome, use "unknown". The local JSONL always logs. The
remote binary only runs if telemetry is not off and the binary exists.

## REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | \`/plan-ceo-review\` | Scope & strategy | 0 | — | — |
| Codex Review | \`/codex review\` | Independent 2nd opinion | 0 | — | — |
| Eng Review | \`/plan-eng-review\` | Architecture & tests (required) | 0 | — | — |
| Design Review | \`/plan-design-review\` | UI/UX gaps | 0 | — | — |

**VERDICT:** NO REVIEWS YET — run \`/autoplan\` for full review pipeline, or individual reviews above.
\`\`\`

**PLAN MODE EXCEPTION — ALWAYS RUN:** This writes to the plan file, which is the one
file you are allowed to edit in plan mode. The plan file review report is part of the
plan's living status.

# /checkpoint — Save and Resume Working State

You are a **Staff Engineer who keeps meticulous session notes**. Your job is to
capture the full working context — what's being done, what decisions were made,
what's left — so that any future session (even on a different branch or workspace)
can resume without losing a beat.

**HARD GATE:** Do NOT implement code changes. This skill captures and restores
context only.

---

## Detect command

Parse the user's input to determine which command to run:

- `/checkpoint` or `/checkpoint save` → **Save**
- `/checkpoint resume` → **Resume**
- `/checkpoint list` → **List**

If the user provides a title after the command (e.g., `/checkpoint auth refactor`),
use it as the checkpoint title. Otherwise, infer a title from the current work.

---

## Save flow

### Step 1: Gather state

Collect the current working state:

```bash
echo "=== BRANCH ==="
git rev-parse --abbrev-ref HEAD 2>/dev/null
echo "=== STATUS ==="
git status --short 2>/dev/null
echo "=== DIFF STAT ==="
git diff --stat 2>/dev/null
echo "=== STAGED DIFF STAT ==="
git diff --cached --stat 2>/dev/null
echo "=== RECENT LOG ==="
git log --oneline -10 2>/dev/null
```

### Step 2: Summarize context

Using the gathered state plus your conversation history, produce a summary covering:

1. **What's being worked on** — the high-level goal or feature
2. **Decisions made** — architectural choices, trade-offs, approaches chosen and why
3. **Remaining work** — concrete next steps, in priority order
4. **Notes** — anything a future session needs to know (gotchas, blocked items,
   open questions, things that were tried and didn't work)

If the user provided a title, use it. Otherwise, infer a concise title (3-6 words)
from the work being done.

### Step 3: Compute session duration

Try to determine how long this session has been active:

```bash
# Try _TEL_START (session timestamp) first, then shell process start time
if [ -n "$_TEL_START" ]; then
  START_EPOCH="$_TEL_START"
elif [ -n "$PPID" ]; then
  START_EPOCH=$(ps -o lstart= -p $PPID 2>/dev/null | xargs -I{} date -jf "%c" "{}" "+%s" 2>/dev/null || echo "")
fi
if [ -n "$START_EPOCH" ]; then
  NOW=$(date +%s)
  DURATION=$((NOW - START_EPOCH))
  echo "SESSION_DURATION_S=$DURATION"
else
  echo "SESSION_DURATION_S=unknown"
fi
```

If the duration cannot be determined, omit the `session_duration_s` field from the
checkpoint file.

### Step 4: Write checkpoint file

Write the checkpoint file to `{CHECKPOINT_DIR}/{TIMESTAMP}-{title-slug}.md` where
`title-slug` is the title in kebab-case (lowercase, spaces replaced with hyphens,
special characters removed).

The file format:

```markdown
---
status: in-progress
branch: {current branch name}
timestamp: {ISO-8601 timestamp, e.g. 2026-03-31T14:30:00-07:00}
session_duration_s: {computed duration, omit if unknown}
files_modified:
  - path/to/file1
  - path/to/file2
---

## Working on: {title}

### Summary

{1-3 sentences describing the high-level goal and current progress}

### Decisions Made

{Bulleted list of architectural choices, trade-offs, and reasoning}

### Remaining Work

{Numbered list of concrete next steps, in priority order}

### Notes

{Gotchas, blocked items, open questions, things tried that didn't work}
```

The `files_modified` list comes from `git status --short` (both staged and unstaged
modified files). Use relative paths from the repo root.

After writing, confirm to the user:

```
CHECKPOINT SAVED
════════════════════════════════════════
Title:    {title}
Branch:   {branch}
File:     {path to checkpoint file}
Modified: {N} files
Duration: {duration or "unknown"}
════════════════════════════════════════
```

---

## Resume flow

### Step 1: Find checkpoints

List checkpoints from **all branches** (checkpoint files contain the branch name
in their frontmatter, so all files in the directory are candidates). This enables
session handoff — a checkpoint saved on one branch can be resumed from
another.

### Step 2: Load checkpoint

If the user specified a checkpoint (by number, title fragment, or date), find the
matching file. Otherwise, load the **most recent** checkpoint.

Read the checkpoint file and present a summary:

```
RESUMING CHECKPOINT
════════════════════════════════════════
Title:       {title}
Branch:      {branch from checkpoint}
Saved:       {timestamp, human-readable}
Duration:    Last session was {formatted duration} (if available)
Status:      {status}
════════════════════════════════════════

### Summary
{summary from checkpoint}

### Remaining Work
{remaining work items from checkpoint}

### Notes
{notes from checkpoint}
```

If the current branch differs from the checkpoint's branch, note this:
"This checkpoint was saved on branch `{branch}`. You are currently on
`{current branch}`. You may want to switch branches before continuing."

### Step 3: Offer next steps

After presenting the checkpoint, ask via AskUserQuestion:

- A) Continue working on the remaining items
- B) Show the full checkpoint file
- C) Just needed the context, thanks

If A, summarize the first remaining work item and suggest starting there.

---

## List flow

### Step 1: Gather checkpoints

### Step 2: Display table

**Default behavior:** Show checkpoints for the **current branch** only.

If the user passes `--all` (e.g., `/checkpoint list --all`), show checkpoints
from **all branches**.

Read the frontmatter of each checkpoint file to extract `status`, `branch`, and
`timestamp`. Parse the title from the filename (the part after the timestamp).

Present as a table:

```
CHECKPOINTS ({branch} branch)
════════════════════════════════════════
#  Date        Title                    Status
─  ──────────  ───────────────────────  ───────────
1  2026-03-31  auth-refactor            in-progress
2  2026-03-30  api-pagination           completed
3  2026-03-28  db-migration-setup       in-progress
════════════════════════════════════════
```

If `--all` is used, add a Branch column:

```
CHECKPOINTS (all branches)
════════════════════════════════════════
#  Date        Title                    Branch              Status
─  ──────────  ───────────────────────  ──────────────────  ───────────
1  2026-03-31  auth-refactor            feat/auth           in-progress
2  2026-03-30  api-pagination           main                completed
3  2026-03-28  db-migration-setup       feat/db-migration   in-progress
════════════════════════════════════════
```

If there are no checkpoints, tell the user: "No checkpoints saved yet. Run
`/checkpoint` to save your current working state."

---

## Important Rules

- **Never modify code.** This skill only reads state and writes checkpoint files.
- **Always include the branch name** in checkpoint files — this is critical for
  cross-branch resume in session workspaces.
- **Checkpoint files are append-only.** Never overwrite or delete existing checkpoint
  files. Each save creates a new file.
- **Infer, don't interrogate.** Use git state and conversation context to fill in
  the checkpoint. Only use AskUserQuestion if the title genuinely cannot be inferred.

---
name: design-shotgun
aliases: ["design-shotgun", "design-variants", "visual-exploration"]
version: 1.0.0
description: |
  Design shotgun: generate multiple AI design variants, open a comparison board,
  collect structured feedback, and iterate. Standalone design exploration you can
  run anytime. Use when: "explore designs", "show me options", "design variants",
  "visual brainstorm", or "I don't like how this looks".
  Proactively suggest when the user describes a UI feature but hasn't seen
  what it could look like.
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Glob
  - Grep
---


# /design-shotgun: Visual Design Exploration

You are a design brainstorming partner. Generate multiple AI design variants, open them
side-by-side in the user's browser, and iterate until they approve a direction. This is
visual brainstorming, not a review process.

## Step 0: Session Detection

Check for prior design exploration sessions for this project. Look for any previously
approved design variants (approved.json files or saved variant PNGs).

**If previous sessions found:** Read each approved record, display a summary, then
ask the user:

> "Previous design explorations for this project:
> - [date]: [screen] — chose variant [X], feedback: '[summary]'
>
> A) Revisit — reopen the comparison board to adjust your choices
> B) New exploration — start fresh with new or updated instructions
> C) Something else"

If A: regenerate the board from existing variant PNGs, reopen, and resume the feedback loop.
If B: proceed to Step 1.

**If no previous sessions:** Show the first-time message:

"This is /design-shotgun — your visual brainstorming tool. I'll generate multiple AI
design directions, open them side-by-side in your browser, and you pick your favorite.
You can run /design-shotgun anytime during development to explore design directions for
any part of your product. Let's start."

## Step 1: Context Gathering

When design-shotgun is invoked from plan-design-review, design-consultation, or another
skill, the calling skill has already gathered context. If a design brief is already
available, skip to Step 2.

When run standalone, gather context to build a proper design brief.

**Required context (5 dimensions):**
1. **Who** — who is the design for? (persona, audience, expertise level)
2. **Job to be done** — what is the user trying to accomplish on this screen/page?
3. **What exists** — what's already in the codebase? (existing components, pages, patterns)
4. **User flow** — how do users arrive at this screen and where do they go next?
5. **Edge cases** — long names, zero results, error states, mobile, first-time vs power user

**Auto-gather first:**

```bash
cat DESIGN.md 2>/dev/null | head -80 || echo "NO_DESIGN_MD"
```

```bash
ls src/ app/ pages/ components/ 2>/dev/null | head -30
```

If DESIGN.md exists, tell the user: "I'll follow your design system in DESIGN.md by
default. If you want to go off the reservation on visual direction, just say so —
design-shotgun will follow your lead, but won't diverge by default."

**Check for a live site to screenshot** (for the "I don't like THIS" use case):

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:3000 2>/dev/null || echo "NO_LOCAL_SITE"
```

If a local site is running AND the user referenced a URL or said something like "I don't
like how this looks," use BrowserNavigate, BrowserScreenshot tools to capture the current
page and use it as the basis for generating improvement variants from the existing design.

**Ask the user with pre-filled context:** Pre-fill what you inferred from the codebase,
DESIGN.md, and other context. Then ask for what's missing. Frame as ONE question
covering all gaps:

> "Here's what I know: [pre-filled context]. I'm missing [gaps].
> Tell me: [specific questions about the gaps].
> How many variants? (default 3, up to 8 for important screens)"

Two rounds max of context gathering, then proceed with what you have and note assumptions.

## Step 2: Taste Memory

Read prior approved designs to bias generation toward the user's demonstrated taste.
If prior sessions exist, read each approved record and extract patterns from the
approved variants. Include a taste summary in the design brief:

"The user previously approved designs with these characteristics: [high contrast,
generous whitespace, modern sans-serif typography, etc.]. Bias toward this aesthetic
unless the user explicitly requests a different direction."

Limit to last 10 sessions. Skip corrupted files.

## Step 3: Generate Variants

Set up the output directory for this design exploration session.

### Step 3a: Concept Generation

Before generating images, create N text concepts describing each variant's design direction.
Each concept should be a distinct creative direction, not a minor variation. Present them
as a lettered list:

```
I'll explore 3 directions:

A) "Name" — one-line visual description of this direction
B) "Name" — one-line visual description of this direction
C) "Name" — one-line visual description of this direction
```

Draw on DESIGN.md, taste memory, and the user's request to make each concept distinct.

### Step 3b: Concept Confirmation

Ask the user to confirm before generating:

> "These are the {N} directions I'll generate."

Options:
- A) Generate all {N} — looks good
- B) I want to change some concepts (tell me which)
- C) Add more variants (I'll suggest additional directions)
- D) Fewer variants (tell me which to drop)

If B: incorporate feedback, re-present concepts, re-confirm. Max 2 rounds.
If C: add concepts, re-present, re-confirm.
If D: drop specified concepts, re-present, re-confirm.

### Step 3c: Generation

Use GenerateImage tool or create HTML mockups directly for each variant. Each variant
should be a distinct visual direction based on the confirmed concepts.

For the "I don't like THIS" path (evolving from an existing screenshot), use the
captured screenshot as the starting point and generate improvement variants.

### Step 3d: Results

After all variants are generated:

1. Show each generated variant inline (Read tool) so the user sees all variants at once.
2. Report status: "All {N} variants generated. {successes} succeeded,
   {failures} failed."
3. For any failures: report explicitly with the error. Do NOT silently skip.
4. Proceed to Step 4 (comparison board).

## Step 4: Comparison Board + Feedback Loop

### Comparison Board

Create an HTML comparison board that displays all generated variants side by side.
Write a self-contained HTML file with:
- All variant images displayed in a grid or side-by-side layout
- Labels (A, B, C, etc.) for each variant
- Brief description of each variant's design direction

Open the comparison board in the user's browser using BrowserNavigate, BrowserClick,
BrowserScreenshot tools for navigation.

**Wait for the user's feedback:**

"I've opened a comparison board with the design variants. Rate them, leave comments,
and tell me your preference. Which variant do you prefer? Any specific feedback?"

**After receiving feedback:** Output a clear summary confirming what was understood:

"Here's what I understood from your feedback:
PREFERRED: Variant [X]
YOUR NOTES: [comments]
DIRECTION: [overall direction]

Is this right?"

Verify before proceeding.

### Regeneration Loop

If the user wants changes:
1. Generate new variants incorporating feedback
2. Update the comparison board
3. Reload using BrowserNavigate tools
4. Ask for feedback again
5. Repeat until the user is satisfied

**Save the approved choice** as an `approved.json` record with:
- Approved variant identifier
- User feedback text
- Date (ISO 8601)
- Screen name
- Current branch

## Step 5: Feedback Confirmation

After receiving feedback, output a clear summary confirming what was understood:

"Here's what I understood from your feedback:

PREFERRED: Variant [X]
RATINGS: A: 4/5, B: 3/5, C: 2/5
YOUR NOTES: [full text of per-variant and overall comments]
DIRECTION: [regenerate action if any]

Is this right?"

Confirm before saving.

## Step 6: Save & Next Steps

Save the approved choice record.

If invoked from another skill: return the structured feedback for that skill to consume.

If standalone, offer next steps:

> "Design direction locked in. What's next?
> A) Iterate more — refine the approved variant with specific feedback
> B) Finalize — generate production Pretext-native HTML/CSS with /design-html
> C) Save to plan — add this as an approved mockup reference in the current plan
> D) Done — I'll use this later"

## Important Rules

1. **Show variants inline before opening the board.** The user should see designs
   immediately in their terminal. The browser board is for detailed feedback.
2. **Confirm feedback before saving.** Always summarize what you understood and verify.
3. **Taste memory is automatic.** Prior approved designs inform new generations by default.
4. **Two rounds max on context gathering.** Don't over-interrogate. Proceed with assumptions.
5. **DESIGN.md is the default constraint.** Unless the user says otherwise.

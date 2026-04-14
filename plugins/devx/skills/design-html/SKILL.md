---
name: design-html
aliases: ["design-html", "html-design", "frontend-design"]
version: 1.0.0
description: |
  Design finalization: generates production-quality Pretext-native HTML/CSS.
  Works with approved mockups from /design-shotgun, design review context
  from /plan-design-review, or from scratch with a user description.
  Text actually reflows, heights are computed, layouts are dynamic.
  30KB overhead, zero deps. Smart API routing: picks the right Pretext patterns
  for each design type. Use when: "finalize this design", "turn this into HTML",
  "build me a page", "implement this design", or after any planning skill.
  Proactively suggest when user has approved a design or has a plan ready.
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Glob
  - Grep
---


# /design-html: Pretext-Native HTML Engine

You generate production-quality HTML where text actually works correctly. Not CSS
approximations. Computed layout via Pretext. Text reflows on resize, heights adjust
to content, cards size themselves, chat bubbles shrinkwrap, editorial spreads flow
around obstacles.

## Step 0: Input Detection

Detect what design context exists for this project. Check for:

1. **Approved mockups** from /design-shotgun (approved variant PNGs in the project)
2. **Design review notes** from /plan-design-review
3. **DESIGN.md** in the repo root (design tokens and constraints)
4. **Existing finalized HTML** from a previous run

Now route based on what was found. Check these cases in order:

### Case A: Approved mockup exists (design-shotgun ran)

If an approved design was found, read it. Extract: approved variant PNG path, user feedback,
screen name. Also read any plan context if it exists (it adds strategic context).

Read `DESIGN.md` if it exists in the repo root. These tokens take priority for
system-level values (fonts, brand colors, spacing scale).

Then check for prior finalized HTML. If found, ask the user:
> Found a prior finalized HTML from a previous session. Want to evolve it
> (apply new changes on top, preserving your custom edits) or start fresh?
> A) Evolve — iterate on the existing HTML
> B) Start fresh — regenerate from the approved mockup

If evolve: read the existing HTML. Apply changes on top during Step 3.
If fresh or no finalized HTML: proceed to Step 1 with the approved PNG as the
visual reference.

### Case B: Plan and/or design variants exist, but no approved mockup

If a plan or variants were found but no approved mockup:

Read whichever context exists:
- If plan found: read it and summarize the product vision and design requirements.
- If variant PNGs found: show them inline using the Read tool.
- If DESIGN.md found: read it for design tokens and constraints.

Ask the user:
> Found [plan | design review variants | both] but no approved design mockup.
> A) Run /design-shotgun — explore design variants based on the existing plan context
> B) Skip mockups — I'll design the HTML directly from the plan context
> C) I have a PNG — let me provide the path

If A: tell the user to run /design-shotgun, then come back to /design-html.
If B: proceed to Step 1 in "plan-driven mode." There is no approved PNG, the plan is
the source of truth. Ask the user for a screen name to use for the output directory
(e.g., "landing-page", "dashboard", "pricing").
If C: accept a PNG file path from the user and proceed with that as the reference.

### Case C: Nothing found (clean slate)

If none of the above produced any context:

Ask the user:
> No design context found for this project. How do you want to start?
> A) Run /plan-design-review first — design review with visual mockups
> B) Run /design-shotgun — jump straight to visual design exploration
> C) Just describe it — tell me what you want and I'll design the HTML live

If A or B: tell the user to run that skill, then come back to /design-html.
If C: proceed to Step 1 in "freeform mode." Ask the user for a screen name.

### Context summary

After routing, output a brief context summary:
- **Mode:** approved-mockup | plan-driven | freeform | evolve
- **Visual reference:** path to approved PNG, or "none (plan-driven)" or "none (freeform)"
- **Design tokens:** "DESIGN.md" or "none"
- **Screen name:** from approved data, user-provided, or inferred from plan

---

## Step 1: Design Analysis

1. If an approved mockup PNG exists, read it inline using the Read tool.
   Describe the visual layout, colors, typography, and component structure.

2. If in plan-driven or freeform mode (no approved PNG), design from context:
   - **Plan-driven:** read the plan and/or design review notes. Extract the described
     UI requirements, user flows, target audience, visual feel (dark/light, dense/spacious),
     content structure (hero, features, pricing, etc.), and design constraints. Build an
     implementation spec from the plan's prose rather than a visual reference.
   - **Freeform:** ask the user about:
     purpose/audience, visual feel (dark/light, playful/serious, dense/spacious),
     content structure (hero, features, pricing, etc.), and any reference sites they like.
   In both cases, describe the intended visual layout, colors, typography, and
   component structure as your implementation spec. Generate realistic content based
   on the plan or user description (never lorem ipsum).

3. Read `DESIGN.md` tokens. These override any extracted values for system-level
   properties (brand colors, font family, spacing scale).

4. Output an "Implementation spec" summary: colors (hex), fonts (family + weights),
   spacing scale, component list, layout type.

---

## Step 2: Smart Pretext API Routing

Analyze the approved design and classify it into a Pretext tier. Each tier uses
different Pretext APIs for optimal results:

| Design type | Pretext APIs | Use case |
|-------------|-------------|----------|
| Simple layout (landing, marketing) | `prepare()` + `layout()` | Resize-aware heights |
| Card/grid (dashboard, listing) | `prepare()` + `layout()` | Self-sizing cards |
| Chat/messaging UI | `prepareWithSegments()` + `walkLineRanges()` | Tight-fit bubbles, min-width |
| Content-heavy (editorial, blog) | `prepareWithSegments()` + `layoutNextLine()` | Text around obstacles |
| Complex editorial | Full engine + `layoutWithLines()` | Manual line rendering |

State the chosen tier and why. Reference the specific Pretext APIs that will be used.

---

## Step 2.5: Framework Detection

Check if the user's project uses a frontend framework:

```bash
[ -f package.json ] && cat package.json | grep -o '"react"\|"svelte"\|"vue"\|"@angular/core"\|"solid-js"\|"preact"' | head -1 || echo "NONE"
```

If a framework is detected, ask the user:
> Detected [React/Svelte/Vue] in your project. What format should the output be?
> A) Vanilla HTML — self-contained preview file (recommended for first pass)
> B) [React/Svelte/Vue] component — framework-native with Pretext hooks

If the user chooses framework output, ask one follow-up:
> A) TypeScript
> B) JavaScript

For vanilla HTML: proceed to Step 3 with vanilla output.
For framework output: proceed to Step 3 with framework-specific patterns.
If no framework detected: default to vanilla HTML, no question needed.

---

## Step 3: Generate Pretext-Native HTML

### Pretext Source Embedding

For **vanilla HTML output**, use CDN import:
`<script type="module">import { prepare, layout, prepareWithSegments, walkLineRanges, layoutNextLine, layoutWithLines } from 'https://esm.sh/@chenglou/pretext'</script>`

For **framework output**, add to the project's dependencies instead:
```bash
# Detect package manager
[ -f bun.lockb ] && echo "bun add @chenglou/pretext" || \
[ -f pnpm-lock.yaml ] && echo "pnpm add @chenglou/pretext" || \
[ -f yarn.lock ] && echo "yarn add @chenglou/pretext" || \
echo "npm install @chenglou/pretext"
```
Run the detected install command. Then use standard imports in the component.

### HTML Generation

Write a single file using the Write tool.

For framework output, save as the appropriate component extension (`.tsx`, `.svelte`, `.vue`).

**Always include in vanilla HTML:**
- Pretext source (CDN import)
- CSS custom properties for design tokens from DESIGN.md / Step 1 extraction
- Google Fonts via `<link>` tags + `document.fonts.ready` gate before first `prepare()`
- Semantic HTML5 (`<header>`, `<nav>`, `<main>`, `<section>`, `<footer>`)
- Responsive behavior via Pretext relayout (not just media queries)
- Breakpoint-specific adjustments at 375px, 768px, 1024px, 1440px
- ARIA attributes, heading hierarchy, focus-visible states
- `contenteditable` on text elements + MutationObserver to re-prepare + re-layout on edit
- ResizeObserver on containers to re-layout on resize
- `prefers-color-scheme` media query for dark mode
- `prefers-reduced-motion` for animation respect
- Real content extracted from the mockup (never lorem ipsum)

**Never include (AI slop blacklist):**
- Purple/blue gradients as default
- Generic 3-column feature grids
- Center-everything layouts with no visual hierarchy
- Decorative blobs, waves, or geometric patterns not in the mockup
- Stock photo placeholder divs
- "Get Started" / "Learn More" generic CTAs not from the mockup
- Rounded-corner cards with drop shadows as the default component
- Emoji as visual elements
- Generic testimonial sections
- Cookie-cutter hero sections with left-text right-image

### Pretext Wiring Patterns

Use these patterns based on the tier selected in Step 2. These are the correct
Pretext API usage patterns. Follow them exactly.

**Pattern 1: Basic height computation (Simple layout, Card/grid)**
```js
import { prepare, layout } from 'https://esm.sh/@chenglou/pretext'

// 1. PREPARE — one-time, after fonts load
await document.fonts.ready
const elements = document.querySelectorAll('[data-pretext]')
const prepared = new Map()

for (const el of elements) {
  const text = el.textContent
  const font = getComputedStyle(el).font
  prepared.set(el, prepare(text, font))
}

// 2. LAYOUT — cheap, call on every resize
function relayout() {
  for (const [el, handle] of prepared) {
    const { height } = layout(handle, el.clientWidth, parseFloat(getComputedStyle(el).lineHeight))
    el.style.height = `${height}px`
  }
}

// 3. RESIZE-AWARE
new ResizeObserver(() => relayout()).observe(document.body)
relayout()

// 4. CONTENT-EDITABLE — re-prepare when text changes
for (const el of elements) {
  if (el.contentEditable === 'true') {
    new MutationObserver(() => {
      const font = getComputedStyle(el).font
      prepared.set(el, prepare(el.textContent, font))
      relayout()
    }).observe(el, { characterData: true, subtree: true, childList: true })
  }
}
```

**Pattern 2: Shrinkwrap / tight-fit containers (Chat bubbles)**
```js
import { prepareWithSegments, walkLineRanges } from 'https://esm.sh/@chenglou/pretext'

// Find the tightest width that produces the same line count
function shrinkwrap(text, font, maxWidth, lineHeight) {
  const segs = prepareWithSegments(text, font)
  let bestWidth = maxWidth
  walkLineRanges(segs, maxWidth, (lineCount, startIdx, endIdx) => {
    // walkLineRanges calls back with progressively narrower widths
    // The first call gives us the line count at maxWidth
    // We want the narrowest width that still produces this line count
  })
  // Binary search for tightest width with same line count
  const { lineCount: targetLines } = layout(prepare(text, font), maxWidth, lineHeight)
  let lo = 0, hi = maxWidth
  while (hi - lo > 1) {
    const mid = (lo + hi) / 2
    const { lineCount } = layout(prepare(text, font), mid, lineHeight)
    if (lineCount === targetLines) hi = mid
    else lo = mid
  }
  return hi
}
```

**Pattern 3: Text around obstacles (Editorial layout)**
```js
import { prepareWithSegments, layoutNextLine } from 'https://esm.sh/@chenglou/pretext'

function layoutAroundObstacles(text, font, containerWidth, lineHeight, obstacles) {
  const segs = prepareWithSegments(text, font)
  let state = null
  let y = 0
  const lines = []

  while (true) {
    // Calculate available width at current y position, accounting for obstacles
    let availWidth = containerWidth
    for (const obs of obstacles) {
      if (y >= obs.top && y < obs.top + obs.height) {
        availWidth -= obs.width
      }
    }

    const result = layoutNextLine(segs, state, availWidth, lineHeight)
    if (!result) break

    lines.push({ text: result.text, width: result.width, x: 0, y })
    state = result.state
    y += lineHeight
  }

  return { lines, totalHeight: y }
}
```

**Pattern 4: Full line-by-line rendering (Complex editorial)**
```js
import { prepareWithSegments, layoutWithLines } from 'https://esm.sh/@chenglou/pretext'

const segs = prepareWithSegments(text, font)
const { lines, height } = layoutWithLines(segs, containerWidth, lineHeight)

// lines = [{ text, width, x, y }, ...]
// Use for Canvas/SVG rendering or custom DOM positioning
for (const line of lines) {
  const span = document.createElement('span')
  span.textContent = line.text
  span.style.position = 'absolute'
  span.style.left = `${line.x}px`
  span.style.top = `${line.y}px`
  container.appendChild(span)
}
```

### Pretext API Reference

```
PRETEXT API CHEATSHEET:

prepare(text, font) → handle
  One-time text measurement. Call after document.fonts.ready.
  Font: CSS shorthand like '16px Inter' or 'bold 24px Georgia'.

layout(prepared, maxWidth, lineHeight) → { height, lineCount }
  Fast layout computation. Call on every resize. Sub-millisecond.

prepareWithSegments(text, font) → handle
  Like prepare() but enables line-level APIs below.

layoutWithLines(segs, maxWidth, lineHeight) → { lines: [{text, width, x, y}...], height }
  Full line-by-line breakdown. For Canvas/SVG rendering.

walkLineRanges(segs, maxWidth, onLine) → void
  Calls onLine(lineCount, startIdx, endIdx) for each possible layout.
  Find minimum width for N lines. For tight-fit containers.

layoutNextLine(segs, state, maxWidth, lineHeight) → { text, width, state } | null
  Iterator. Different maxWidth per line = text around obstacles.
  Pass null as initial state. Returns null when text is exhausted.

clearCache() → void
  Clears internal measurement caches. Use when cycling many fonts.

setLocale(locale?) → void
  Retargets word segmenter for future prepare() calls.
```

---

## Step 3.5: Live Reload Server

After writing the HTML file, start a simple HTTP server for live preview:

```bash
# Start a simple HTTP server in the output directory
_OUTPUT_DIR=$(dirname <path-to-finalized.html>)
cd "$_OUTPUT_DIR"
python3 -m http.server 0 --bind 127.0.0.1 &
_SERVER_PID=$!
_PORT=$(lsof -i -P -n | grep "$_SERVER_PID" | grep LISTEN | awk '{print $9}' | cut -d: -f2 | head -1)
echo "SERVER: http://localhost:$_PORT/finalized.html"
echo "PID: $_SERVER_PID"
```

If python3 is not available, fall back to:
```bash
open <path-to-finalized.html>
```

Tell the user: "Live preview running at http://localhost:$_PORT/finalized.html.
After each edit, just refresh the browser (Cmd+R) to see changes."

When the refinement loop ends (Step 4 exits), kill the server:
```bash
kill $_SERVER_PID 2>/dev/null || true
```

---

## Step 4: Preview + Refinement Loop

### Verification Screenshots

Use BrowserNavigate, BrowserClick, BrowserScreenshot tools to take verification
screenshots at 3 viewports:

1. Navigate to `file://<path-to-finalized.html>`
2. Screenshot at mobile width (375px)
3. Screenshot at tablet width (768px)
4. Screenshot at desktop width (1440px)

Show all three screenshots inline. Check for:
- Text overflow (text cut off or extending beyond containers)
- Layout collapse (elements overlapping or missing)
- Responsive breakage (content not adapting to viewport)

If issues are found, note them and fix before presenting to the user.

If browser tools are not available, skip verification and note:
"Browser tools not available. Skipping automated viewport verification."

### Refinement Loop

```
LOOP:
  1. If server is running, tell user to open http://localhost:PORT/finalized.html
     Otherwise: open <path>/finalized.html

  2. If an approved mockup PNG exists, show it inline (Read tool) for visual comparison.
     If in plan-driven or freeform mode, skip this step.

  3. Ask the user (adjust wording based on mode):
     With mockup: "The HTML is live in your browser. Here's the approved mockup for comparison.
      Try: resize the window (text should reflow dynamically),
      click any text (it's editable, layout recomputes instantly).
      What needs to change? Say 'done' when satisfied."
     Without mockup: "The HTML is live in your browser. Try: resize the window
      (text should reflow dynamically), click any text (it's editable, layout
      recomputes instantly). What needs to change? Say 'done' when satisfied."

  4. If "done" / "ship it" / "looks good" / "perfect" → exit loop, go to Step 5

  5. Apply feedback using targeted Edit tool changes on the HTML file
     (do NOT regenerate the entire file — surgical edits only)

  6. Brief summary of what changed (2-3 lines max)

  7. If verification screenshots are available, re-take them to confirm the fix

  8. Go to LOOP
```

Maximum 10 iterations. If the user hasn't said "done" after 10, ask:
"We've done 10 rounds of refinement. Want to continue iterating or call it done?"

---

## Step 5: Save & Next Steps

### Design Token Extraction

If no `DESIGN.md` exists in the repo root, offer to create one from the generated HTML:

Extract from the HTML:
- CSS custom properties (colors, spacing, font sizes)
- Font families and weights used
- Color palette (primary, secondary, accent, neutral)
- Spacing scale
- Border radius values
- Shadow values

Ask the user:
> No DESIGN.md found. I can extract the design tokens from the HTML we just built
> and create a DESIGN.md for your project. This means future /design-shotgun and
> /design-html runs will be style-consistent automatically.
> A) Create DESIGN.md from these tokens
> B) Skip — I'll handle the design system later

If A: write `DESIGN.md` to the repo root with the extracted tokens.

### Next Steps

Ask the user:
> Design finalized with Pretext-native layout. What's next?
> A) Copy to project — copy the HTML/component into your codebase
> B) Iterate more — keep refining
> C) Done — I'll use this as a reference

---

## Important Rules

- **Source of truth fidelity over code elegance.** When an approved mockup exists,
  pixel-match it. If that requires `width: 312px` instead of a CSS grid class, that's
  correct. When in plan-driven or freeform mode, the user's feedback during the
  refinement loop is the source of truth. Code cleanup happens later during
  component extraction.

- **Always use Pretext for text layout.** Even if the design looks simple, Pretext
  ensures correct height computation on resize. The overhead is 30KB. Every page benefits.

- **Surgical edits in the refinement loop.** Use the Edit tool to make targeted changes,
  not the Write tool to regenerate the entire file. The user may have made manual edits
  via contenteditable that should be preserved.

- **Real content only.** When a mockup exists, extract text from it. In plan-driven mode,
  use content from the plan. In freeform mode, generate realistic content based on the
  user's description. Never use "Lorem ipsum", "Your text here", or placeholder content.

- **One page per invocation.** For multi-page designs, run /design-html once per page.
  Each run produces one HTML file.

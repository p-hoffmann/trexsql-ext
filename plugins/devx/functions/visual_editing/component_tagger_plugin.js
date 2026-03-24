/**
 * DevX Component Tagger Vite Plugin
 * Injects data-devx-id and data-devx-name attributes into JSX elements at dev time.
 * This file runs in the user's Vite process — must be plain JS, no TypeScript.
 *
 * Usage in vite.config.ts:
 *   import devxComponentTagger from './.devx/component_tagger_plugin.js';
 *   export default defineConfig({ plugins: [devxComponentTagger(), react()] });
 */

export default function devxComponentTagger() {
  return {
    name: "vite-plugin-devx-tagger",
    apply: "serve", // dev only
    enforce: "pre",

    async transform(code, id) {
      // Skip non-JSX/TSX files
      if (!/\.[jt]sx$/.test(id)) return null;
      // Skip node_modules
      if (/node_modules/.test(id)) return null;

      try {
        // Use magic-string for efficient source transformations with sourcemap
        const { default: MagicString } = await import("magic-string");
        const s = new MagicString(code);
        const relPath = getRelativePath(id);

        // Regex-based approach: find JSX opening tags like <ComponentName or <div
        // Match: < followed by a letter (not </ or < followed by space/=)
        // We look for patterns like <TagName and inject attributes
        const jsxOpeningTagRegex = /<([A-Z][A-Za-z0-9_.]*|[a-z][a-z0-9-]*)\s/g;
        let match;
        let modified = false;

        while ((match = jsxOpeningTagRegex.exec(code)) !== null) {
          const tagName = match[1];
          const insertPos = match.index + match[0].length;

          // Skip TypeScript generics: if the < is preceded by a word char it's
          // a generic type param (e.g. useState<Todo, Array<Item) not JSX.
          if (match.index > 0 && /\w/.test(code[match.index - 1])) continue;

          // Calculate line:col from offset
          const lines = code.slice(0, match.index).split("\n");
          const line = lines.length;
          const col = lines[lines.length - 1].length + 1;

          // Only tag user components (PascalCase) — skip intrinsic HTML elements
          // for cleaner selection (HTML elements inherit from nearest component)
          if (/^[A-Z]/.test(tagName)) {
            const devxId = relPath + ":" + line + ":" + col;
            const attrs =
              'data-devx-id="' + devxId + '" data-devx-name="' + tagName + '" ';
            s.appendLeft(insertPos, attrs);
            modified = true;
          }
        }

        if (!modified) return null;

        return {
          code: s.toString(),
          map: s.generateMap({ hires: true }),
        };
      } catch (err) {
        // Don't break the build if tagger fails
        console.warn("[devx-tagger] Failed to process " + id + ":", err.message);
        return null;
      }
    },
  };
}


function getRelativePath(id) {
  // Strip the project root to get a relative path
  // Vite provides absolute paths; we want src/components/Button.tsx
  const cwd = process.cwd().replace(/\\/g, "/");
  const normalized = id.replace(/\\/g, "/");
  if (normalized.startsWith(cwd + "/")) {
    return normalized.slice(cwd.length + 1);
  }
  return normalized;
}

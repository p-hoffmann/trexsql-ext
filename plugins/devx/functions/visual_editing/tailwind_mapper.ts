// @ts-nocheck - Deno edge function
/**
 * Maps CSS property+value pairs to Tailwind CSS classes.
 * Handles standard Tailwind spacing scale and falls back to arbitrary values.
 */

// Tailwind spacing scale: value in px → Tailwind suffix
const SPACING_SCALE: Record<string, string> = {
  "0px": "0",
  "1px": "px",
  "2px": "0.5",
  "4px": "1",
  "6px": "1.5",
  "8px": "2",
  "10px": "2.5",
  "12px": "3",
  "14px": "3.5",
  "16px": "4",
  "20px": "5",
  "24px": "6",
  "28px": "7",
  "32px": "8",
  "36px": "9",
  "40px": "10",
  "44px": "11",
  "48px": "12",
  "56px": "14",
  "64px": "16",
  "80px": "20",
  "96px": "24",
};

const BORDER_RADIUS_SCALE: Record<string, string> = {
  "0px": "rounded-none",
  "2px": "rounded-sm",
  "4px": "rounded",
  "6px": "rounded-md",
  "8px": "rounded-lg",
  "12px": "rounded-xl",
  "16px": "rounded-2xl",
  "24px": "rounded-3xl",
  "9999px": "rounded-full",
};

const BORDER_WIDTH_SCALE: Record<string, string> = {
  "0px": "border-0",
  "1px": "border",
  "2px": "border-2",
  "4px": "border-4",
  "8px": "border-8",
};

const FONT_SIZE_SCALE: Record<string, string> = {
  "12px": "text-xs",
  "14px": "text-sm",
  "16px": "text-base",
  "18px": "text-lg",
  "20px": "text-xl",
  "24px": "text-2xl",
  "30px": "text-3xl",
  "36px": "text-4xl",
  "48px": "text-5xl",
  "60px": "text-6xl",
};

const FONT_WEIGHT_SCALE: Record<string, string> = {
  "100": "font-thin",
  "200": "font-extralight",
  "300": "font-light",
  "400": "font-normal",
  "500": "font-medium",
  "600": "font-semibold",
  "700": "font-bold",
  "800": "font-extrabold",
  "900": "font-black",
};

// Common Tailwind colors (subset for matching)
const TAILWIND_COLORS: Record<string, string> = {
  "#000000": "black",
  "#ffffff": "white",
  "#ef4444": "red-500",
  "#f97316": "orange-500",
  "#eab308": "yellow-500",
  "#22c55e": "green-500",
  "#3b82f6": "blue-500",
  "#6366f1": "indigo-500",
  "#8b5cf6": "violet-500",
  "#a855f7": "purple-500",
  "#ec4899": "pink-500",
  "#64748b": "slate-500",
  "#6b7280": "gray-500",
  "#f8fafc": "slate-50",
  "#f1f5f9": "slate-100",
  "#e2e8f0": "slate-200",
  "#cbd5e1": "slate-300",
  "#94a3b8": "slate-400",
  "#475569": "slate-600",
  "#334155": "slate-700",
  "#1e293b": "slate-800",
  "#0f172a": "slate-900",
};

type SpacingDirection = "top" | "right" | "bottom" | "left";

function spacingClass(
  type: "m" | "p",
  direction: SpacingDirection,
  value: string,
): string {
  const dirMap: Record<SpacingDirection, string> = {
    top: "t",
    right: "r",
    bottom: "b",
    left: "l",
  };
  const prefix = type + dirMap[direction];
  const scale = SPACING_SCALE[value];
  if (scale !== undefined) {
    return `${prefix}-${scale}`;
  }
  return `${prefix}-[${value}]`;
}

function colorClass(prefix: string, value: string): string {
  const hex = normalizeColor(value);
  if (hex && TAILWIND_COLORS[hex]) {
    return `${prefix}-${TAILWIND_COLORS[hex]}`;
  }
  if (hex) {
    return `${prefix}-[${hex}]`;
  }
  return `${prefix}-[${value}]`;
}

function normalizeColor(value: string): string | null {
  // Handle hex colors
  if (/^#[0-9a-fA-F]{3,8}$/.test(value)) {
    return value.toLowerCase();
  }
  // Handle rgb/rgba
  const rgbMatch = value.match(
    /rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/,
  );
  if (rgbMatch) {
    const r = parseInt(rgbMatch[1]).toString(16).padStart(2, "0");
    const g = parseInt(rgbMatch[2]).toString(16).padStart(2, "0");
    const b = parseInt(rgbMatch[3]).toString(16).padStart(2, "0");
    return `#${r}${g}${b}`;
  }
  return null;
}

/** Conflicting class prefixes for deduplication */
const SPACING_PREFIXES = [
  "mt-", "mr-", "mb-", "ml-", "mx-", "my-", "m-",
  "pt-", "pr-", "pb-", "pl-", "px-", "py-", "p-",
];
const STYLE_PREFIXES = [
  "border-", "rounded-", "bg-", "text-", "font-",
  ...SPACING_PREFIXES,
];

/**
 * Remove conflicting classes from an existing className string.
 * E.g., if adding "pt-4", remove any existing "pt-*" class.
 */
export function deduplicateClasses(
  existingClasses: string,
  newClasses: string[],
): string {
  const existing = existingClasses.split(/\s+/).filter(Boolean);
  const prefixesToRemove = new Set<string>();

  for (const newClass of newClasses) {
    // Find which prefix this new class matches
    for (const prefix of STYLE_PREFIXES) {
      if (newClass.startsWith(prefix) || newClass === prefix.slice(0, -1)) {
        prefixesToRemove.add(prefix);
        break;
      }
    }
    // Handle exact matches like "rounded" or "border"
    if (newClass === "rounded" || newClass.startsWith("rounded-")) {
      prefixesToRemove.add("rounded");
    }
    if (newClass === "border" || newClass.match(/^border-\d/)) {
      prefixesToRemove.add("border-");
      prefixesToRemove.add("border");
    }
  }

  const filtered = existing.filter((cls) => {
    for (const prefix of prefixesToRemove) {
      if (cls.startsWith(prefix) || cls === prefix.slice(0, -1)) {
        return false;
      }
    }
    return true;
  });

  // Deduplicate within newClasses — keep last occurrence per prefix
  const deduped: string[] = [];
  for (let i = newClasses.length - 1; i >= 0; i--) {
    const cls = newClasses[i];
    const hasConflict = deduped.some((existing) => {
      for (const prefix of STYLE_PREFIXES) {
        if (cls.startsWith(prefix) && existing.startsWith(prefix)) return true;
      }
      return false;
    });
    if (!hasConflict) deduped.unshift(cls);
  }

  return [...filtered, ...deduped].join(" ");
}

export interface StyleChanges {
  margin?: { top?: string; right?: string; bottom?: string; left?: string };
  padding?: { top?: string; right?: string; bottom?: string; left?: string };
  border?: { width?: string; radius?: string; color?: string };
  backgroundColor?: string;
  text?: { fontSize?: string; fontWeight?: string; color?: string };
}

/**
 * Convert a StyleChanges object into an array of Tailwind classes.
 */
export function stylesToTailwindClasses(changes: StyleChanges): string[] {
  const classes: string[] = [];

  if (changes.margin) {
    for (const [dir, val] of Object.entries(changes.margin)) {
      if (val) classes.push(spacingClass("m", dir as SpacingDirection, val));
    }
  }

  if (changes.padding) {
    for (const [dir, val] of Object.entries(changes.padding)) {
      if (val) classes.push(spacingClass("p", dir as SpacingDirection, val));
    }
  }

  if (changes.border) {
    if (changes.border.width) {
      const bw = BORDER_WIDTH_SCALE[changes.border.width];
      classes.push(bw || `border-[${changes.border.width}]`);
    }
    if (changes.border.radius) {
      const br = BORDER_RADIUS_SCALE[changes.border.radius];
      classes.push(br || `rounded-[${changes.border.radius}]`);
    }
    if (changes.border.color) {
      classes.push(colorClass("border", changes.border.color));
    }
  }

  if (changes.backgroundColor) {
    classes.push(colorClass("bg", changes.backgroundColor));
  }

  if (changes.text) {
    if (changes.text.fontSize) {
      const fs = FONT_SIZE_SCALE[changes.text.fontSize];
      classes.push(fs || `text-[${changes.text.fontSize}]`);
    }
    if (changes.text.fontWeight) {
      const fw = FONT_WEIGHT_SCALE[changes.text.fontWeight];
      classes.push(fw || `font-[${changes.text.fontWeight}]`);
    }
    if (changes.text.color) {
      classes.push(colorClass("text", changes.text.color));
    }
  }

  return classes;
}

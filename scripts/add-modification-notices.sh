#!/usr/bin/env bash
# add-modification-notices.sh
#
# Apache-2.0 §4(b) compliance helper for the Trex repo.
#
# For a given fork directory and an upstream git ref (the unmodified baseline),
# identify files that have been changed locally and prepend a "Modified by Trex"
# notice in the file's appropriate comment syntax.
#
# Usage:
#   scripts/add-modification-notices.sh <fork-dir> <upstream-ref> [--check] [--year YYYY] [--owner "Supabase, Inc."] [--license "Apache-2.0"]
#
#   --check     Do not write anything. Exit non-zero if any modified file lacks
#               the required notice. Intended for CI.
#   --year      Year to embed in the notice (default: current year).
#   --owner     Original copyright holder (default: "Supabase, Inc.").
#   --license   License name to reference (default: "Apache-2.0").
#
# Idempotent: files that already contain "Modified by Trex contributors" are skipped.

set -euo pipefail

usage() {
  sed -n '2,20p' "$0" >&2
  exit 2
}

CHECK=0
YEAR="$(date +%Y)"
OWNER="Supabase, Inc."
LICENSE="Apache-2.0"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK=1; shift ;;
    --year) YEAR="$2"; shift 2 ;;
    --owner) OWNER="$2"; shift 2 ;;
    --license) LICENSE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done

if [[ ${#POSITIONAL[@]} -ne 2 ]]; then
  usage
fi

FORK_DIR="${POSITIONAL[0]}"
UPSTREAM_REF="${POSITIONAL[1]}"

if [[ ! -d "$FORK_DIR/.git" && ! -f "$FORK_DIR/.git" ]]; then
  echo "error: $FORK_DIR is not a git working tree" >&2
  exit 2
fi

MARKER="Modified by Trex contributors"

# Comment-style metadata per extension.
# style:
#   line   -> single-line comment with given prefix
#   block  -> wrapped in <prefix> ... <suffix>
#   skip   -> never touch
comment_style() {
  local f="$1"
  local base ext name
  base="$(basename "$f")"
  name="${base%.*}"
  ext="${base##*.}"
  # Files without an extension fall back to the basename match (e.g. "Dockerfile").
  if [[ "$ext" == "$base" ]]; then ext=""; fi

  case "$ext" in
    ts|tsx|js|jsx|mjs|cjs|rs|go|c|cc|cpp|h|hpp|java|kt|swift|scala|css|scss)
      echo "line //" ;;
    py|sh|bash|zsh|yml|yaml|toml|conf|ini|env|cfg|sample)
      echo "line #" ;;
    sql)
      echo "line --" ;;
    html|htm|xml|svg|vue)
      echo "block <!-- -->" ;;
    json|json5|jsonc|md|markdown|lock|txt|csv|tsv|png|jpg|jpeg|gif|webp|ico|woff|woff2|ttf|otf|pdf|zip|tar|gz)
      echo "skip" ;;
    *)
      case "$base" in
        Dockerfile|Makefile|Containerfile|.env*|.gitignore|.dockerignore)
          echo "line #" ;;
        *)
          echo "skip" ;;
      esac
      ;;
  esac
}

# Returns 0 if file already has the marker.
has_marker() {
  local f="$1"
  # only scan the first 20 lines so the script stays fast
  head -n 20 "$f" 2>/dev/null | grep -qF "$MARKER"
}

# Returns 0 if file is binary per git heuristic.
is_binary() {
  local f="$1"
  # `git check-attr` would need a path inside the repo; use a content sniff instead.
  if LC_ALL=C grep -qP '[\x00]' "$f" 2>/dev/null; then
    return 0
  fi
  return 1
}

build_notice() {
  local style="$1" prefix="$2" suffix="${3:-}"
  local line1="Modified by Trex contributors. Original Copyright (c) ${YEAR} ${OWNER%.}."
  local line2="See LICENSE for the original ${LICENSE} license."
  if [[ "$style" == "block" ]]; then
    printf '%s %s\n%s %s %s\n' "$prefix" "$line1" "$prefix" "$line2" "$suffix"
  else
    printf '%s %s\n%s %s\n' "$prefix" "$line1" "$prefix" "$line2"
  fi
}

# Insert the notice after a leading shebang or XML declaration if present.
insert_notice() {
  local f="$1" notice="$2"
  local first_line
  first_line="$(head -n1 "$f" 2>/dev/null || true)"
  local tmp
  tmp="$(mktemp)"
  if [[ "$first_line" == "#!"* || "$first_line" == "<?xml"* ]]; then
    {
      printf '%s\n' "$first_line"
      printf '%s\n' "$notice"
      tail -n +2 "$f"
    } >"$tmp"
  else
    {
      printf '%s\n' "$notice"
      cat "$f"
    } >"$tmp"
  fi
  # Preserve permissions.
  chmod --reference="$f" "$tmp" 2>/dev/null || true
  mv "$tmp" "$f"
}

# Identify modified files using git.
pushd "$FORK_DIR" >/dev/null
mapfile -t MODIFIED < <(git diff --name-only --diff-filter=M "$UPSTREAM_REF" HEAD)
popd >/dev/null

MISSING=()
ADDED=()
SKIPPED=()

for rel in "${MODIFIED[@]}"; do
  [[ -z "$rel" ]] && continue
  f="$FORK_DIR/$rel"
  [[ ! -f "$f" ]] && continue

  # Skip vendored / generated locations.
  case "$rel" in
    node_modules/*|*/node_modules/*|dist/*|*/dist/*|build/*|*/build/*|target/*|*/target/*|.next/*|*/.next/*|coverage/*|*/coverage/*|vendor/*|*/vendor/*)
      SKIPPED+=("$rel"); continue ;;
  esac

  read -r style prefix suffix <<<"$(comment_style "$rel")"
  if [[ "$style" == "skip" ]]; then
    SKIPPED+=("$rel"); continue
  fi

  if is_binary "$f"; then
    SKIPPED+=("$rel"); continue
  fi

  if has_marker "$f"; then
    continue
  fi

  if [[ "$CHECK" -eq 1 ]]; then
    MISSING+=("$rel")
    continue
  fi

  notice="$(build_notice "$style" "$prefix" "${suffix:-}")"
  insert_notice "$f" "$notice"
  ADDED+=("$rel")
done

if [[ "$CHECK" -eq 1 ]]; then
  if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo "Missing Apache-2.0 §4(b) modification notices in ${#MISSING[@]} file(s) under $FORK_DIR:" >&2
    printf '  %s\n' "${MISSING[@]}" >&2
    exit 1
  fi
  echo "OK: all modified files in $FORK_DIR carry a notice."
  exit 0
fi

echo "Added notices to ${#ADDED[@]} file(s) under $FORK_DIR (skipped ${#SKIPPED[@]})."

#!/usr/bin/env bash
#
# gen-release-notes.sh — release notes for a GitHub Release body.
#
# Writes $OUT_DIR/release-notes.md: a human-readable list of commit
# subjects. Source is the git history:
#   * tag runs (CHANNEL=testing|stable): commits since the previous tag
#     (git describe --abbrev=0 --tags <REF>^), i.e. exactly what is in the
#     release;
#   * develop: the last MAX_COMMITS commits.
#
# Env:
#   CHANNEL      develop|testing|stable — required
#   OUT_DIR      output directory (default sds-nbd/dist)
#   MAX_COMMITS  cap for the develop list (default 50)
#   GITHUB_REF_NAME  tag name on tag runs
#
# Idempotent; never fails when there is no tag history yet (first release):
# the list is then built from the repository root.
set -euo pipefail

CHANNEL="${CHANNEL:?CHANNEL is required}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT/sds-nbd/dist}"
MAX_COMMITS="${MAX_COMMITS:-50}"
REF="${GITHUB_REF_NAME:-HEAD}"

cd "$ROOT"

case "$CHANNEL" in
develop)
  RANGE="$REF"
  LIMIT=(-n "$MAX_COMMITS")
  ;;
testing | stable)
  PREV="$(git describe --abbrev=0 --tags "$REF^" 2>/dev/null || true)"
  if [ -n "$PREV" ]; then
    RANGE="$PREV..$REF"
  else
    RANGE="$REF"
  fi
  LIMIT=()
  ;;
*)
  echo "gen-release-notes: unknown CHANNEL=$CHANNEL" >&2
  exit 1
  ;;
esac

mkdir -p "$OUT_DIR"
OUT="$OUT_DIR/release-notes.md"

{
  echo "Изменения:"
  echo
  git log --no-merges --format='- %s' "${LIMIT[@]}" "$RANGE"
} >"$OUT"

echo "wrote $OUT:"
cat "$OUT"

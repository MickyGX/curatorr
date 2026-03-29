#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DRY_RUN=0
VERSION=""
IMAGE="mickygx/curatorr"
PUSH_LATEST=""
PUSH_DEVELOPMENT=""
DO_GIT_PUSH=""
DO_DOCKER_BUILD=""
DO_DOCKER_PUSH=""
DO_GH_RELEASE=""

die() {
  echo "Error: $*" >&2
  exit 1
}

run_cmd() {
  echo "+ $*"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "$@"
  fi
}

prompt_default() {
  local label="$1"
  local default="$2"
  local out
  read -r -p "$label [$default]: " out
  if [[ -z "${out// }" ]]; then
    echo "$default"
  else
    echo "$out"
  fi
}

prompt_yes_no() {
  local label="$1"
  local default="$2"
  local out
  local hint="y/N"
  [[ "$default" == "y" ]] && hint="Y/n"
  while true; do
    read -r -p "$label ($hint): " out
    out="$(printf '%s' "$out" | tr '[:upper:]' '[:lower:]')"
    if [[ -z "$out" ]]; then
      out="$default"
    fi
    case "$out" in
      y|yes) echo "1"; return 0 ;;
      n|no) echo "0"; return 0 ;;
      *) echo "Please answer y or n." ;;
    esac
  done
}

get_current_version() {
  grep -m1 '"version"' package.json | sed -E 's/.*"version"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/'
}

suggest_next_patch() {
  local v="$1"
  IFS='.' read -r major minor patch <<<"$v"
  if [[ -n "${major:-}" && -n "${minor:-}" && -n "${patch:-}" ]]; then
    echo "${major}.${minor}.$((patch + 1))"
  else
    echo "$v"
  fi
}

sanitize_version() {
  local raw="$1"
  local cleaned
  cleaned="$(printf '%s' "$raw" | tr -d '\r' | sed -E 's/^[[:space:]]+//;s/[[:space:]]+$//')"
  cleaned="${cleaned#v}"
  cleaned="${cleaned#V}"
  echo "$cleaned"
}

bump_version() {
  if command -v npm >/dev/null 2>&1; then
    run_cmd npm version "$VERSION" --no-git-tag-version
    return 0
  fi
  if command -v perl >/dev/null 2>&1; then
    run_cmd perl -0777 -i -pe "s/\"version\"\\s*:\\s*\"[^\"]+\"/\"version\": \"$VERSION\"/" package.json
    return 0
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "+ awk -v ver=$VERSION replace package.json version"
    return 0
  fi
  awk -v ver="$VERSION" '
    BEGIN { updated = 0 }
    {
      if (!updated && $0 ~ /"version"[[:space:]]*:[[:space:]]*"[^"]+"/) {
        sub(/"version"[[:space:]]*:[[:space:]]*"[^"]+"/, "\"version\": \"" ver "\"")
        updated = 1
      }
      print
    }
    END {
      if (!updated) exit 2
    }
  ' package.json > package.json.tmp || die "Unable to update version in package.json"
  mv package.json.tmp package.json
}

ensure_release_files() {
  local tag="v$VERSION"
  local release_file="docs/release/releases/${tag}.md"
  local announce_file="docs/release/releases/${tag}-announcement.md"
  local notes_file="docs/release/RELEASE_NOTES.md"
  local today
  today="$(date +%F)"

  run_cmd mkdir -p docs/release/releases
  if [[ ! -f "$release_file" ]]; then
    run_cmd cp docs/release/changelog-template.md "$release_file"
  fi
  if [[ "$DO_GH_RELEASE" -eq 1 && ! -f "$announce_file" ]]; then
    run_cmd cp docs/release/announcement-template.md "$announce_file"
  fi

  if grep -q "^## v${VERSION} " "$notes_file" 2>/dev/null; then
    return 0
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "+ prepend release section to $notes_file for v$VERSION"
    return 0
  fi

  tmp_file="$(mktemp)"
  {
    echo "# Release Notes (v${CURRENT_VERSION} -> v${VERSION})"
    echo
    echo "## v${VERSION} (${today})"
    echo
    echo "- [Added]"
    echo "- [Changed]"
    echo "- [Fixed]"
    echo
    if [[ -f "$notes_file" ]]; then
      sed '1{/^# Release Notes/d;}' "$notes_file"
    fi
  } >"$tmp_file"
  mv "$tmp_file" "$notes_file"
}

usage() {
  cat <<'EOF'
Interactive release helper for Curatorr.

Usage:
  scripts/release-assistant.sh [options]

Options:
  --version X.Y.Z        Set release version.
  --image NAME           Docker image (default: mickygx/curatorr).
  --dry-run              Print commands without executing.
  -h, --help             Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --image)
      IMAGE="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Run inside the git repository."
command -v git >/dev/null 2>&1 || die "git is required."

CURRENT_VERSION="$(get_current_version)"
SUGGESTED_VERSION="$(suggest_next_patch "$CURRENT_VERSION")"
if [[ -z "${VERSION// }" ]]; then
  VERSION="$(prompt_default "Release version" "$SUGGESTED_VERSION")"
fi
VERSION="$(sanitize_version "$VERSION")"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "Version must look like X.Y.Z"

if [[ -z "$PUSH_LATEST" ]]; then
  PUSH_LATEST="$(prompt_yes_no "Tag/push latest?" "y")"
fi
if [[ -z "$PUSH_DEVELOPMENT" ]]; then
  PUSH_DEVELOPMENT="$(prompt_yes_no "Tag/push development?" "n")"
fi
if [[ -z "$DO_GIT_PUSH" ]]; then
  DO_GIT_PUSH="$(prompt_yes_no "Push git commit/tag to origin?" "y")"
fi
if [[ -z "$DO_DOCKER_BUILD" ]]; then
  DO_DOCKER_BUILD="$(prompt_yes_no "Build Docker image?" "y")"
fi
if [[ -z "$DO_DOCKER_PUSH" ]]; then
  DO_DOCKER_PUSH="$(prompt_yes_no "Push Docker tags to Docker Hub?" "y")"
fi
if [[ -z "$DO_GH_RELEASE" ]]; then
  DO_GH_RELEASE="$(prompt_yes_no "Prepare/publish GitHub release?" "n")"
fi

if [[ "$DO_GH_RELEASE" -eq 1 && "$DO_GIT_PUSH" -eq 0 ]]; then
  die "GitHub release requires pushed tag. Enable git push."
fi

GIT_TAG="v$VERSION"
RELEASE_FILE="docs/release/releases/${GIT_TAG}.md"
ANNOUNCE_FILE="docs/release/releases/${GIT_TAG}-announcement.md"

echo
echo "Release plan:"
echo "  Version: $VERSION"
echo "  Git tag: $GIT_TAG"
echo "  Image:   $IMAGE"
echo "  latest tag:      $PUSH_LATEST"
echo "  development tag: $PUSH_DEVELOPMENT"
echo "  git push:        $DO_GIT_PUSH"
echo "  docker build:    $DO_DOCKER_BUILD"
echo "  docker push:     $DO_DOCKER_PUSH"
echo "  gh release:      $DO_GH_RELEASE"
echo "  dry-run:         $DRY_RUN"
echo

CONFIRM="$(prompt_yes_no "Proceed?" "y")"
[[ "$CONFIRM" -eq 1 ]] || exit 0

if [[ "$DRY_RUN" -eq 0 ]]; then
  CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
  STATUS_COUNT="$(git status --porcelain | wc -l | tr -d ' ')"
  EXPECTED_CONFIRM="RELEASE ${GIT_TAG}"
  echo
  echo "Final safety check:"
  echo "  current branch: ${CURRENT_BRANCH}"
  echo "  working tree entries: ${STATUS_COUNT}"
  echo "Type exactly: ${EXPECTED_CONFIRM}"
  read -r -p "Confirmation: " FINAL_CONFIRM
  [[ "$FINAL_CONFIRM" == "$EXPECTED_CONFIRM" ]] || die "Safety confirmation mismatch. Aborting."
fi

run_cmd git checkout main
run_cmd git pull --rebase origin main

bump_version
ensure_release_files

run_cmd git add package.json docs/release/RELEASE_NOTES.md "$RELEASE_FILE"
if [[ -f package-lock.json ]]; then
  run_cmd git add package-lock.json
fi
if [[ -f "$ANNOUNCE_FILE" ]]; then
  run_cmd git add "$ANNOUNCE_FILE"
fi

if [[ "$DRY_RUN" -eq 0 ]]; then
  if git diff --cached --quiet; then
    die "Nothing staged for commit. Update release notes/template files first."
  fi
fi

run_cmd git commit -m "release: ${GIT_TAG}"

if [[ "$DO_GIT_PUSH" -eq 1 ]]; then
  run_cmd git push origin main
  run_cmd git tag -a "$GIT_TAG" -m "$GIT_TAG"
  run_cmd git push origin "$GIT_TAG"
fi

if [[ "$DO_DOCKER_BUILD" -eq 1 ]]; then
  if [[ "$DRY_RUN" -eq 0 ]]; then
    command -v docker >/dev/null 2>&1 || die "docker is required for build/push steps."
  fi
  TAGS=("$VERSION")
  [[ "$PUSH_LATEST" -eq 1 ]] && TAGS+=("latest")
  [[ "$PUSH_DEVELOPMENT" -eq 1 ]] && TAGS+=("development")

  BUILD_CMD=(docker build --build-arg "APP_VERSION=$VERSION")
  for tag in "${TAGS[@]}"; do
    BUILD_CMD+=(-t "${IMAGE}:${tag}")
  done
  BUILD_CMD+=(.)
  run_cmd "${BUILD_CMD[@]}"

  if [[ "$DO_DOCKER_PUSH" -eq 1 ]]; then
    for tag in "${TAGS[@]}"; do
      run_cmd docker push "${IMAGE}:${tag}"
    done
  fi
fi

if [[ "$DO_GH_RELEASE" -eq 1 ]]; then
  if command -v gh >/dev/null 2>&1; then
    run_cmd gh release create "$GIT_TAG" --title "$GIT_TAG" --notes-file "$RELEASE_FILE"
  else
    echo "gh not found. Create GitHub release manually with notes from: $RELEASE_FILE"
  fi
fi

echo
echo "Done."
echo "Release notes file: $RELEASE_FILE"
if [[ -f "$ANNOUNCE_FILE" ]]; then
  echo "Announcement draft: $ANNOUNCE_FILE"
fi

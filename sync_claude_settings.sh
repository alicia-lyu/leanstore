#!/usr/bin/env bash
set -euo pipefail

# Sync project-scope Claude settings (not tracked by git) to a remote machine.
# Usage: ./sync_claude_settings.sh user@host [remote_project_dir]
#
# The remote project dir defaults to the same absolute path as the local project.
# Runs rsync with --delete so the remote always mirrors local state.

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 user@host [remote_project_dir]" >&2
    exit 1
fi

REMOTE="$1"
LOCAL_PROJECT="$(cd "$(dirname "$0")" && pwd)"
REMOTE_PROJECT="${2:-$LOCAL_PROJECT}"

CLAUDE_DIR="$LOCAL_PROJECT/.claude"

if [[ ! -d "$CLAUDE_DIR" ]]; then
    echo "No .claude/ directory in $LOCAL_PROJECT — nothing to sync." >&2
    exit 0
fi

echo "Syncing $CLAUDE_DIR -> $REMOTE:$REMOTE_PROJECT/.claude/"

rsync -avz --delete \
    "$CLAUDE_DIR/" \
    "$REMOTE:$REMOTE_PROJECT/.claude/"

echo "Done."

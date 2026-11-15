#!/usr/bin/env bash
# statusline.sh — OpenClaude status line command: one line about the local
# worker's dispatch state, so an interactive session sees when a bridge task
# arrives and is being executed. Same output contract as the cclocal status
# line (~/.claude-local/statusline-agent-bridge.sh): ids/states/durations
# only, never task text.
#
# Registered from ~/.openclaude/settings.json:
#   "statusLine": {"type": "command",
#                  "command": "<bridge>/systemd/statusline.sh",
#                  "refreshInterval": 5}
# Stock builds ignore refreshInterval (the settings schema strips unknown
# keys) and refresh the line on TUI events only; a build with the
# refreshInterval patch re-runs this command every N seconds.
#
# Always exits 0: on any failure the client simply keeps the previous line.
HERE="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
BRIDGE="$(cd "$HERE/.." && pwd)"
PY="$BRIDGE/.venv/bin/python"
[ -x "$PY" ] || PY=python3
"$PY" "$BRIDGE/systemd/listener.py" progress --agent local --once 2>/dev/null
exit 0

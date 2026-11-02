#!/usr/bin/env bash
# kimi_headless.sh — non-interactive launcher for the official Kimi CLI, the
# symmetric counterpart of cclocal_headless.sh: systemd/on_message.py picks
# the launcher by message.recipient, so a claude->kimi root task wakes a Kimi
# worker the same way a kimi->claude one wakes a cclocal worker.
#
# SECURITY NOTE (documented deliberately, see README): the official Kimi CLI
# in -p/--prompt mode does not ask for approvals and applies its standard
# "auto" permission policy; --prompt is incompatible with --yolo/--auto/--plan,
# so there is no faithful equivalent of claude's --permission-prompts none
# here. No fake permission flag is added and static deny rules are not
# weakened; the worker prompt restricts behavior instead, and automatic
# dispatch for recipient=kimi stays disabled in config.json by default.
#
# The prompt is passed to Kimi as a short instruction naming a 0600 runtime
# request file, never the task text itself, so the payload stays out of argv
# and the process list. Kimi keeps using its own $KIMI_CODE_HOME; project MCP
# servers load from <workspace>/.kimi-code/mcp.json once the workspace is
# trusted (KIMI_CODE_TRUST_WORKSPACE=1 below).
#
# Testability overrides:
#   KIMI_BIN  kimi executable (default ~/.kimi-code/bin/kimi)
set -euo pipefail

KIMI_BIN="${KIMI_BIN:-$HOME/.kimi-code/bin/kimi}"

# Project-scoped MCP (.kimi-code/mcp.json) only loads in a trusted workspace.
export KIMI_CODE_TRUST_WORKSPACE="${KIMI_CODE_TRUST_WORKSPACE:-1}"

exec "$KIMI_BIN" "$@"

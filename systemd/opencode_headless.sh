#!/usr/bin/env bash
# opencode_headless.sh — one headless OpenCode 2 worker (recipient id
# "opencode") per message, started by systemd/on_message.py.
#
# The task prompt arrives on stdin: `opencode run` without a message
# argument reads the whole message from stdin, so the payload never reaches
# argv or the process list.
#
# Everything the worker needs is injected through OPENCODE_CONFIG_CONTENT
# (an extra config source on top of the user's own OpenCode config):
#   * the bridge MCP server, mcp.servers.agent_bridge, with AGENT_ID=opencode
#     and Code Mode off, so its tools are plain agent_bridge_<tool> calls;
#   * the permission rules from systemd/opencode-permissions.json (or
#     $OPENCODE_WORKER_PERMISSIONS). `run` is started WITHOUT --auto: any
#     request those rules leave at "ask" is rejected automatically, and the
#     worker is told so — the honest outcome is a `blocked:` report;
#   * optionally, the local llama-server as an OpenAI-compatible provider
#     (OPENCODE_WORKER_PROVIDER=local), built from the same LOCAL_CLAUDE_*
#     assignments in ~/.bash_aliases as oclocal_headless.sh.
#
# --standalone: a private server for this run. OpenCode 2's background
# service keeps the environment it was started with, so without it the
# injected config and AGENT_ID would not reliably apply. --archived keeps
# worker sessions out of the user's session list.
#
# Settings (environment):
#   OPENCODE_WORKER_MODEL        provider/model[#variant]; empty = OpenCode's
#                                configured default (or the local model when
#                                OPENCODE_WORKER_PROVIDER=local)
#   OPENCODE_WORKER_AGENT        OpenCode agent name (default: its default)
#   OPENCODE_WORKER_PERMISSIONS  JSON array of permission rules
#   OPENCODE_WORKER_PROVIDER     "local" = use the local llama-server
# Testability overrides:
#   OPENCODE_BIN                 opencode executable (default: from PATH)
#   OPENCODE_ENV_FILE            file with LOCAL_CLAUDE_* (default ~/.bash_aliases)
set -euo pipefail

HERE="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
BRIDGE_DIR="${AGENT_BRIDGE_DIR:-$(cd "$HERE/.." && pwd)}"
PERMS="${OPENCODE_WORKER_PERMISSIONS:-$HERE/opencode-permissions.json}"
OPENCODE_BIN="${OPENCODE_BIN:-$(command -v opencode || true)}"
[[ -n "$OPENCODE_BIN" && -x "$OPENCODE_BIN" ]] || {
    echo "opencode_headless: opencode executable not found (set OPENCODE_BIN)" >&2
    exit 127
}
[[ -r "$PERMS" ]] || {
    echo "opencode_headless: permission rules not readable: $PERMS" >&2
    exit 2
}

model="${OPENCODE_WORKER_MODEL:-}"
provider_url=""
provider_model=""
extra_env=()
if [[ "${OPENCODE_WORKER_PROVIDER:-}" == "local" ]]; then
    ENV_FILE="${OPENCODE_ENV_FILE:-$HOME/.bash_aliases}"
    if [[ -r "$ENV_FILE" ]]; then
        # Whitelisted eval, exactly as in oclocal_headless.sh: only the known
        # assignments run, never the rest of an interactive rc file.
        assignments="$(grep -E '^[[:space:]]*(export[[:space:]]+)?(LOCAL_CLAUDE_[A-Z_]+|LOCAL_API_TIMEOUT_MS)=' "$ENV_FILE" || true)"
        if [[ -n "$assignments" ]]; then
            # shellcheck disable=SC2046,SC2086
            eval "$assignments"
        fi
    fi
    key="${LOCAL_CLAUDE_AUTH_TOKEN:-}"
    if [[ -z "$key" && -n "${LOCAL_CLAUDE_KEY_FILE:-}" && -s "$LOCAL_CLAUDE_KEY_FILE" ]]; then
        key="$(< "$LOCAL_CLAUDE_KEY_FILE")"
    fi
    provider_url="${LOCAL_CLAUDE_BASE_URL:?LOCAL_CLAUDE_BASE_URL is not defined in $ENV_FILE}"
    provider_url="${provider_url%/}"
    provider_url="${provider_url%/v1}/v1"
    provider_model="${LOCAL_CLAUDE_MODEL:?LOCAL_CLAUDE_MODEL is not defined in $ENV_FILE}"
    model="${model:-llama/$provider_model}"
    host="${provider_url#*://}"
    host="${host%%[:/]*}"
    np="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost,$host"
    # The key travels in the environment only; the config refers to it as
    # {env:OPENCODE_LOCAL_API_KEY} and never contains it.
    extra_env+=(NO_PROXY="$np" no_proxy="$np"
                OPENCODE_LOCAL_API_KEY="${key:-local-dummy}")
fi

config="$(python3 -I - "$BRIDGE_DIR" "$PERMS" "$provider_url" "$provider_model" <<'PY'
import json, sys
bridge, perms, url, model = sys.argv[1:5]
with open(perms, encoding="utf-8") as fh:
    rules = json.load(fh)
if not isinstance(rules, list) or not all(
        isinstance(r, dict) and {"action", "resource", "effect"} <= set(r)
        for r in rules):
    sys.exit("opencode_headless: permissions must be a list of"
             " {action, resource, effect} rules")
cfg = {
    "$schema": "https://opencode.ai/config.json",
    "mcp": {"servers": {"agent_bridge": {
        "type": "local",
        "command": ["uv", "run", "--directory", bridge, "python", "server.py"],
        "environment": {"AGENT_ID": "opencode"},
        "codemode": False,
    }}},
    "permissions": rules,
}
if url:
    cfg["providers"] = {"llama": {
        "name": "llama-server (agent_bridge worker)",
        "package": "aisdk:@ai-sdk/openai-compatible",
        "settings": {"baseURL": url, "apiKey": "{env:OPENCODE_LOCAL_API_KEY}"},
        "models": {model: {"name": model}},
    }}
print(json.dumps(cfg, ensure_ascii=False))
PY
)"

args=(run --standalone --archived --format json)
[[ -n "$model" ]] && args+=(--model "$model")
[[ -n "${OPENCODE_WORKER_AGENT:-}" ]] && args+=(--agent "$OPENCODE_WORKER_AGENT")

# AGENT_BRIDGE_WORKER=1 marks a bridge worker: manager-mode hooks never block
# it (systemd/manager_mode.py).
exec env \
    ${extra_env[@]+"${extra_env[@]}"} \
    OPENCODE_CONFIG_CONTENT="$config" \
    AGENT_BRIDGE_WORKER=1 \
    "$OPENCODE_BIN" "${args[@]}" "$@"

#!/usr/bin/env bash
# oclocal_headless.sh — non-interactive replacement for the `oclocal` shell
# function from ~/.bash_aliases, used by systemd/on_message.py to run one
# headless OpenClaude worker (recipient id "local") per message. Rebuilds the
# same environment (OpenAI-compatible endpoint of the local llama-server)
# and execs openclaude with the given args.
#
# The auth token is read from the environment or the key file and passed only
# through the child process environment: it is never printed, never logged
# and never written to the DB, logs or the prompt.
#
# Testability overrides:
#   OCLOCAL_ENV_FILE      file with the LOCAL_CLAUDE_* assignments (default ~/.bash_aliases)
#   OCLOCAL_OPENCLAUDE_BIN openclaude executable (default: openclaude from PATH)
set -euo pipefail

# AGENT_BRIDGE_DIR: systemd/mcp.local.json references the bridge directory as
# "${AGENT_BRIDGE_DIR}" — OpenClaude expands it against this launcher's env.
# Derived from the script's own location, so the repo layout is never baked
# in and any clone location works without edits.
HERE="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
export AGENT_BRIDGE_DIR="${AGENT_BRIDGE_DIR:-$(cd "$HERE/.." && pwd)}"

ENV_FILE="${OCLOCAL_ENV_FILE:-$HOME/.bash_aliases}"
OPENCLAUDE_BIN="${OCLOCAL_OPENCLAUDE_BIN:-openclaude}"

if [[ -r "$ENV_FILE" ]]; then
    # Whitelisted eval: only assignments of the known variables are executed,
    # so arbitrary interactive rc code never runs in this launcher.
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
key="${key:-local-dummy}"

url="${LOCAL_CLAUDE_BASE_URL:-http://192.168.2.15:8080}"
url="${url%/}/v1"
host="${url#*://}"
host="${host%%[:/]*}"
np="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost,$host"

# exec env … — mirrors oclocal(): Anthropic vars of the parent are removed so
# the OpenAI-compatible endpoint is the only provider the child can see.
exec env \
    -u ANTHROPIC_BASE_URL -u ANTHROPIC_AUTH_TOKEN -u ANTHROPIC_API_KEY \
    NO_PROXY="$np" no_proxy="$np" \
    CLAUDE_CODE_USE_OPENAI=1 \
    OPENAI_BASE_URL="$url" \
    OPENAI_API_KEY="$key" \
    OPENAI_MODEL="$LOCAL_CLAUDE_MODEL" \
    CLAUDE_CODE_AUTO_COMPACT_WINDOW="$LOCAL_CLAUDE_AUTO_COMPACT" \
    API_TIMEOUT_MS="$LOCAL_API_TIMEOUT_MS" \
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
    "$OPENCLAUDE_BIN" "$@"

#!/usr/bin/env bash
# cclocal_headless.sh — non-interactive replacement for the `cclocal` shell
# function from ~/.bash_aliases, used by systemd/on_message.py to run one
# headless claude worker per message. Rebuilds the same environment and execs
# claude with the given args. Symmetric counterpart for the other side of the
# bridge is kimi_headless.sh.
#
# The auth token is read from the environment or the key file and passed only
# through the child process environment: it is never printed, never logged
# and never written to the DB, logs or the prompt.
#
# Testability overrides:
#   CCLOCAL_ENV_FILE   file with the LOCAL_CLAUDE_* assignments (default ~/.bash_aliases)
#   CCLOCAL_CLAUDE_BIN claude executable (default ~/.local/bin/claude)
set -euo pipefail

ENV_FILE="${CCLOCAL_ENV_FILE:-$HOME/.bash_aliases}"
CLAUDE_BIN="${CCLOCAL_CLAUDE_BIN:-$HOME/.local/bin/claude}"

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
auth_token="$key"
api_key=""
if [[ "${LOCAL_CLAUDE_USE_API_KEY:-0}" == "1" ]]; then
    auth_token=""
    api_key="$key"
fi

host="${LOCAL_CLAUDE_BASE_URL:-localhost}"
host="${host#*://}"
host="${host%%[:/]*}"
np="${NO_PROXY:+$NO_PROXY,}127.0.0.1,localhost,$host"

mkdir -p "${LOCAL_CLAUDE_CONFIG_DIR:?LOCAL_CLAUDE_CONFIG_DIR is not defined in $ENV_FILE}"

exec env \
    NO_PROXY="$np" no_proxy="$np" \
    CLAUDE_CONFIG_DIR="$LOCAL_CLAUDE_CONFIG_DIR" \
    ANTHROPIC_BASE_URL="${LOCAL_CLAUDE_BASE_URL:?LOCAL_CLAUDE_BASE_URL is not defined in $ENV_FILE}" \
    ANTHROPIC_AUTH_TOKEN="$auth_token" \
    ANTHROPIC_API_KEY="$api_key" \
    ANTHROPIC_MODEL="${LOCAL_CLAUDE_MODEL:-}" \
    ANTHROPIC_DEFAULT_OPUS_MODEL="${LOCAL_CLAUDE_MODEL:-}" \
    ANTHROPIC_DEFAULT_SONNET_MODEL="${LOCAL_CLAUDE_MODEL:-}" \
    ANTHROPIC_DEFAULT_HAIKU_MODEL="${LOCAL_CLAUDE_FAST_MODEL:-}" \
    ANTHROPIC_SMALL_FAST_MODEL="${LOCAL_CLAUDE_FAST_MODEL:-}" \
    CLAUDE_CODE_AUTO_COMPACT_WINDOW="${LOCAL_CLAUDE_AUTO_COMPACT:-}" \
    API_TIMEOUT_MS="${LOCAL_API_TIMEOUT_MS:-}" \
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
    "$CLAUDE_BIN" "$@"

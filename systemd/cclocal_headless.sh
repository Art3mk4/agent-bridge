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
#   CCLOCAL_STRIP_UNRECOGNIZED_MODEL
#                      1 (default) drops the CLI's [claude-code:unrecognized_model]
#                      diagnostic from the worker output; 0 passes it through
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

# claude_env -- the client environment, rebuilt on every run so an interactive
# shell is never required. Kept as a function only so the two output paths below
# share one definition; it takes the claude argv and is the last thing started.
claude_env() {
    env \
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
}

# The endpoint serves a GGUF model id that is absent from the CLI's built-in
# alias table, so the CLI prefixes every worker run with one diagnostic line,
# `[claude-code:unrecognized_model] {...}` (TODO P2-4). The model sent in the
# request is left exactly as configured — same id, same endpoint, same weights:
# only that CLI diagnostic is dropped from the merged stream the supervisor
# captures into worker_<id>.out. The pattern is anchored at the line start, so a
# worker report quoting the tag survives, and --line-buffered keeps partial
# output on disk when a worker is killed on timeout. claude's exit status is
# propagated: _classify() reads it from dispatch_attempts.exit_code. The filter
# costs one extra shell in the worker's process group, which the supervisor
# already kills as a group (start_new_session + killpg).
if [[ "${CCLOCAL_STRIP_UNRECOGNIZED_MODEL:-1}" == "1" ]]; then
    status=0
    claude_env "$@" 2>&1 \
        | { grep -v --line-buffered -E '^\[claude-code:unrecognized_model\]' || [[ $? == 1 ]]; } \
        || status=$?
    exit "$status"
fi

claude_env "$@"

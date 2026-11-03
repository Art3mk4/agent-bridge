# Agent Bridge

Local MCP message bridge for **Claude Code** and **Kimi CLI**.

Agent Bridge lets two coding agents exchange tasks, results and bounded autonomous conversations through one shared SQLite database. It uses MCP stdio transport and local files only — no TCP/UDP server, no external service and no network protocol of its own.

The reference Claude Code setup runs the Claude-side worker against **Strata**, model `qwen3.8-flash-next-unsloth-ud-q4_k_xl`, through the local launcher. The bridge core itself is model-agnostic and can dispatch work to any compatible agent client.

> **Status: experimental.** The main exchange flow is implemented and covered by tests, but the project is still evolving. See [TODO.md](TODO.md) for known gaps.

## Stack

- **Claude Code worker:** local headless Claude Code launcher, configured for **Strata**, model `qwen3.8-flash-next-unsloth-ud-q4_k_xl`.
- **Kimi CLI worker:** official Kimi CLI in prompt mode.
- **Agent Bridge core:** Python 3.11+, MCP stdio transport and SQLite.

## Highlights

- **Two-way agent inbox:** send, receive, acknowledge and reply to messages.
- **Safe concurrent workers:** atomic claims with expiring leases prevent two workers from executing the same task.
- **Bounded conversations:** goal-driven `task → result` dialogues with a turn limit, expiry, server-side ordering and reviewer decisions.
- **Automatic headless delivery:** per-agent listeners can dispatch root tasks and conversation turns to one-shot worker processes.
- **Progress indication:** a compact listener status shows the current lifecycle stage and the last dispatch result without exposing task payload.
- **Anti-spam primitives:** response deadlines, idempotency keys and no automatic retries.
- **Payload-safe diagnostics:** logs and status files contain ids, stages and exit codes — not prompts, message text, tokens or worker output.
- **Single-file state:** the shared message database is plain SQLite with WAL mode and busy timeouts.

## How it works

```text
Claude Code session ── MCP stdio ──► server.py (AGENT_ID=claude) ─┐
                                                                   ├──► bridge.db
Kimi CLI session ───── MCP stdio ──► server.py (AGENT_ID=kimi) ───┘
                                        ▲
                                        │ scan / dispatch
                              systemd/listener.py
                                        │
                              systemd/on_message.py
                                        │
                         one-shot Claude or Kimi worker
```

Each MCP server process derives its sender identity from `AGENT_ID`; tool arguments cannot impersonate another sender. Both sides point at the same `bridge.db`, but use opposite values of `AGENT_ID`. Agent ids are fixed (`claude` and `kimi`, `AGENTS` in `server.py`); the name `cclocal` elsewhere in this README is only the local launcher/integration name and the report prefix for the `claude` side — `AGENT_ID=cclocal` is rejected.

Interactive MCP sessions do not receive push notifications: an active agent checks `receive_messages` when it is already running. The optional listener adds the headless path: it scans the database, runs `on_message.py` for one dispatchable message and waits until that supervisor exits before scanning again.

## Requirements

- Linux or another POSIX environment
- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Claude Code and/or Kimi CLI, depending on the side being automated

## Quick start

```bash
git clone <your-repository-url>
cd agent-bridge
uv sync
```

Run the safe dispatcher test suite:

```bash
uv run python systemd/test_dispatcher.py
```

Start a bridge server manually for development:

```bash
AGENT_ID=claude uv run python server.py
# in another terminal:
AGENT_ID=kimi uv run python server.py
```

`server.py` is an MCP stdio process, so normally it is launched by the agent client rather than used directly as an interactive command.

## MCP registration

Use an absolute repository path. Registration is project-scoped; restart the agent session after adding or changing the MCP server.

### Claude Code

```bash
claude mcp add-json agent_bridge -s project '{
  "command": "uv",
  "args": [
    "run",
    "--directory",
    "/absolute/path/to/agent-bridge",
    "python",
    "server.py"
  ],
  "env": {
    "AGENT_ID": "claude"
  }
}'
```

Start a new Claude Code session, approve the project-scoped server and check that eight `agent_bridge` tools are available.

### Kimi CLI

Add the following to the project-scoped Kimi MCP configuration, changing the absolute path as needed:

```json
{
  "mcpServers": {
    "agent_bridge": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/absolute/path/to/agent-bridge",
        "python",
        "server.py"
      ],
      "env": {
        "AGENT_ID": "kimi"
      },
      "enabled": true,
      "startupTimeoutMs": 30000,
      "toolTimeoutMs": 40000,
      "enabledTools": [
        "send_message",
        "receive_messages",
        "claim_message",
        "release_message",
        "mark_read",
        "start_conversation",
        "conversation",
        "listener"
      ]
    }
  }
}
```

Start a new Kimi session from the project and trust the workspace if the MCP command is expected.

## Tools

| Tool | Purpose |
|---|---|
| `send_message` | Store a message, optionally linked with `reply_to`, deadline and `dedup_key`. |
| `receive_messages` | Return unread messages for the current `AGENT_ID`; optionally long-poll up to 30 seconds. |
| `claim_message` | Atomically claim or renew a message lease before headless execution. |
| `release_message` | Return an unfinished claim to the shared inbox without acknowledging it. |
| `mark_read` | Acknowledge completed messages; only the recipient can acknowledge. |
| `start_conversation` | Create a bounded dialogue and send its root `task`. |
| `conversation` | Inspect, complete, cancel or escalate a dialogue. |
| `listener` | Enable, disable or inspect the current agent's background listener; `action="progress"` returns the same one-line progress string as the cclocal status line. |

## Basic exchange

```text
1. sender:   send_message(recipient, text, dedup_key="task-1")
2. recipient: receive_messages()
3. recipient: claim_message(message_id)
4. recipient: send_message(sender, result, reply_to=message_id,
                            dedup_key="task-1:result")
5. recipient: mark_read([message_id])
6. sender:    receive_messages()
7. sender:    mark_read([result_id])
```

If the recipient cannot finish, it sends a `blocked: ...` result and releases the original message instead of acknowledging it. A response deadline is advisory metadata: Agent Bridge never auto-resends and never retries an attempted dispatch, and a passed deadline does not stop a never-attempted message from being delivered.

## Bounded conversations

`start_conversation(recipient, goal, max_turns=6, expires_in_seconds=14400)` creates a dialogue with a fixed goal, a turn budget and an expiry time. Messages inside the conversation use `kind`:

- `task` asks the other side to perform the next step;
- `result` reports completion, blockage or a request for clarification;
- `notice` carries operational information without waking a worker or consuming a turn.

The server enforces pairing and direction: a `result` answers a `task`, a follow-up `task` answers a `result`, and only one message may be in flight. The reviewer side must choose exactly one terminal action after a result: complete the conversation, send one concrete follow-up task or escalate to `needs_human`.

Terminal conversations accept no new tasks or results. Hard dispatch failures freeze the dialogue so a broken chain cannot continue automatically.

## Automatic delivery and progress

The default automation gate is:

```json
{
  "auto": {
    "claude": true,
    "kimi": false
  },
  "dispatch_max_age_seconds": 86400
}
```

Claude-side headless execution is enabled by default in this configuration. Kimi-side headless execution remains disabled because Kimi CLI prompt mode applies its own automatic permission policy without interactive approvals; enable `auto.kimi` only as a deliberate local decision.

`dispatch_max_age_seconds` (default 24 h) bounds automatic delivery by message **age**. A message that arrived while the listener was stopped, restarting or busy with another worker is still dispatched when it comes back, even if its `response_deadline` has passed — the deadline is metadata for the worker and the human. Only a backlog older than the bound is left alone: `listener.py pending` lists it and a manual `on_message.py <id>` runs one message on purpose.

A malformed `config.json` automates nothing (fail closed).

Useful commands:

```bash
uv run python systemd/listener.py status   --agent claude
uv run python systemd/listener.py progress --agent claude --once
uv run python systemd/listener.py watch    --agent claude
uv run python systemd/listener.py pending  --agent claude
uv run python systemd/listener.py disable  --agent claude
uv run python systemd/listener.py enable   --agent claude
```

`listener status` keeps the original machine-readable fields and additionally reports:

- `stage`: `stopped`, `unknown`, `idle`, `received` or `supervising`;
- `progress`: the currently trusted lifecycle record;
- `last`: the previous dispatch outcome, including duration and exit code where available.

`listener pending` is a read-only backlog report: never-attempted messages that could wake a worker, split into `dispatchable` (the listener will take them) and `stale` (older than `dispatch_max_age_seconds`). It prints ids and ages only and never writes to the database.

From an MCP session the same human-readable snapshot is one call: `listener(action="progress")` returns `{"line": ...}` — the exact string the cclocal status line renders, e.g. `[claude] idle | last msg#43 sender=kimi result=succeeded in 21.5s rc=0`. It is ids, enums and numbers only, never task payload, and the call never raises.

Progress files live in `systemd/runtime/<agent>.progress.json` and contain ids, enums and numbers only. The runtime directory is private and gitignored.

### Claude Code status line

There is no supported API for injecting an arbitrary external event into a live Claude Code session. A supported read-side option is Claude Code's custom status line: the command runs on a timer and its stdout becomes the status line.

Create the script in the config directory your interactive sessions actually use — for this machine's cclocal setup that is `~/.claude-local` (`CLAUDE_CONFIG_DIR=$HOME/.claude-local`); on a default Claude Code install it is `~/.claude`. Use absolute paths inside, make it executable, and keep it exit 0 unconditionally — a status line command that fails or hangs degrades the whole session UI:

```bash
#!/usr/bin/env bash
# ~/.claude-local/statusline-agent-bridge.sh — one line about the claude listener.
BRIDGE=/absolute/path/to/agent-bridge
PY="$BRIDGE/.venv/bin/python"
[ -x "$PY" ] || PY=python3
"$PY" "$BRIDGE/systemd/listener.py" progress --agent claude --once 2>/dev/null
exit 0
```

```bash
chmod +x ~/.claude-local/statusline-agent-bridge.sh
```

Then add this key to the `settings.json` in that same config directory, leaving every other key untouched:

```json
{
  "statusLine": {
    "type": "command",
    "command": "/home/<user>/.claude-local/statusline-agent-bridge.sh",
    "refreshInterval": 2
  }
}
```

The line carries only ids, stage words, sender names, durations and exit codes. Right after a listener restart, before the first dispatch, it is just:

```text
[claude] idle
```

Once work flows, the same command grows the current-dispatch and history segments:

```text
[claude] supervising msg#33 sender=kimi supervisor=564040 for 12s
[claude] idle | last msg#33 sender=kimi result=blocked in 938.7s rc=0
```

**Limitation:** the status line is visible only inside interactive Claude Code sessions. Outside a session there is no indication — no desktop notification, sound or dbus integration is provided, by design. A running session may not pick up `settings.json` changes mid-flight; open a new session to see the line.

This is optional local configuration; Agent Bridge does not modify global Claude settings by itself — enabling it is a deliberate per-machine decision.

## Worker launchers

The supervisor chooses a launcher by recipient:

- `systemd/cclocal_headless.sh` starts a non-interactive Claude Code worker. It is an example integration for a local Claude-compatible wrapper and expects the local environment variables used by that wrapper.
- `systemd/kimi_headless.sh` starts the official Kimi CLI in prompt mode and passes the task through a private request file so task text does not appear in argv or the process list.

For public or team use, review these launchers and adapt them to your own client wrappers and permission model. Do not weaken static deny rules or hide permission requests.

Workers run in the **workspace** directory — the project both CLIs open, where their project MCP configs live (`.mcp.json` for the Claude worker's `--strict-mcp-config`, `.kimi-code/mcp.json` for Kimi). By default it is the directory that *contains* this repository, which fits a layout like `~/project/agent-bridge`. If the clone lives elsewhere, set `AGENT_BRIDGE_WORKSPACE` in the `env` of both MCP registrations; the listener and its supervisors inherit it.

If a launcher cannot be started at all (missing file, wrong permissions, bad workspace), the attempt is recorded as `failed` with `error_kind=spawn:<Error>`, the sender gets the usual payload-free notice and a conversation is frozen — the message is not left silently in `spawned`.

## Reliability model

- **One attempt barrier:** `dispatch_attempts.message_id` is the primary key. A rescan, restart or duplicate listener cannot launch the same message twice.
- **No automatic retry:** crash, timeout or uncertainty remains visible for a human decision; a short operational notice is sent without task payload.
- **Evidence-based success:** exit code 0 alone is not success. The protocol requires an acknowledged original plus the appropriate result, blocked report, follow-up or terminal conversation state.
- **Lease and timeout:** worker claims expire; a worker timeout kills only that worker process group.
- **Delivery window:** never-attempted messages are dispatched regardless of `response_deadline`, within `dispatch_max_age_seconds`; older ones wait for a human.
- **Crash recovery:** stale attempts are reconciled by process evidence and report evidence. A recorded worker pid only counts as alive when `/proc` shows that process started together with its attempt, so a recycled pid cannot pin an attempt in `spawned`. `--recover` examines only messages that have never been attempted and uses the same dispatch set as the listener.
- **Concurrent start:** every process switches the database to WAL through the same retried recipe (`systemd/schema.py`), because on a not-yet-WAL file SQLite rejects the switch immediately while another writer holds a lock.
- **Fresh database bootstrap:** the listener creates the dispatcher schema additively before scanning, so a new bridge database does not require a manual supervisor run.

## Security notes

- The bridge is local: MCP stdio, SQLite and local subprocesses.
- Do not commit `bridge.db`, runtime files, outbox reports or agent MCP configuration.
- Secrets are passed through environment variables or private files and are never written to the database, prompts, logs or progress files.
- Logs are operational metadata only and are created `0600` (`dispatcher.log` by both the listener and the supervisor, `errors.log` by the server). Do not redirect MCP stderr into a file that also stores secret-bearing diagnostics.
- Claude-side workers may run with permission prompts disabled. Keep their allowlist minimal and prefer an explicit `blocked` response over any uncertain action.
- Kimi-side automatic dispatch is deliberately gated off in the default configuration.
- Conversations are not authorization to perform external or irreversible actions. Deployments, pushes, purchases and messages to third parties require a human decision.

## Development

Safe tests use temporary databases and fake launchers/listeners/workers:

```bash
uv run python systemd/test_dispatcher.py
```

`test_bridge.py` is a destructive end-to-end test that deletes and recreates `bridge.db`. Run it only in a disposable copy of the project before live traffic, never against a database containing real messages.

Current safe suite: 56 dispatcher tests, including a deterministic stress test for concurrent supervisors against a non-WAL database held by a writer.

`systemd/schema.py` is the shared core of the dispatcher: DDL, the connection recipe, the `config.json` reader and the single definition of the dispatch set used by both the listener and `--recover`. Change dispatch rules there, not in the callers.

## Known limitations

See [TODO.md](TODO.md). The main open items are operational: minimal permissions for headless workers and visibility of `blocked` outcomes.

## License

[MIT](LICENSE)

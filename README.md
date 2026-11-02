# Agent Bridge

Local MCP message bridge for **Claude Code** and **Kimi CLI**.

Agent Bridge lets two coding agents exchange tasks, results and bounded autonomous conversations through one shared SQLite database. It uses MCP stdio transport and local files only — no TCP/UDP server, no external service and no network protocol of its own.

> **Status: experimental.** The main exchange flow is implemented and covered by tests, but the project is still evolving. See [TODO.md](TODO.md) for known gaps.

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

Each MCP server process derives its sender identity from `AGENT_ID`; tool arguments cannot impersonate another sender. Both sides point at the same `bridge.db`, but use opposite values of `AGENT_ID`.

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
| `listener` | Enable, disable or inspect the current agent's background listener. |

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

If the recipient cannot finish, it sends a `blocked: ...` result and releases the original message instead of acknowledging it. A response deadline is advisory metadata: Agent Bridge never auto-resends and never retries an attempted dispatch.

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
  }
}
```

Claude-side headless execution is enabled by default in this configuration. Kimi-side headless execution remains disabled because Kimi CLI prompt mode applies its own automatic permission policy without interactive approvals; enable `auto.kimi` only as a deliberate local decision.

Useful commands:

```bash
uv run python systemd/listener.py status   --agent claude
uv run python systemd/listener.py progress --agent claude --once
uv run python systemd/listener.py watch    --agent claude
uv run python systemd/listener.py disable  --agent claude
uv run python systemd/listener.py enable   --agent claude
```

`listener status` keeps the original machine-readable fields and additionally reports:

- `stage`: `stopped`, `unknown`, `idle`, `received` or `supervising`;
- `progress`: the currently trusted lifecycle record;
- `last`: the previous dispatch outcome, including duration and exit code where available.

Progress files live in `systemd/runtime/<agent>.progress.json` and contain ids, enums and numbers only. The runtime directory is private and gitignored.

### Claude Code status line

There is no supported API for injecting an arbitrary external event into a live Claude Code session. A supported read-side option is Claude Code's custom status line:

```bash
#!/usr/bin/env bash
# ~/.claude/statusline-agent-bridge.sh
uv run --directory /absolute/path/to/agent-bridge \
  python systemd/listener.py progress --agent claude 2>/dev/null
```

```json
{
  "statusLine": {
    "type": "command",
    "command": "~/.claude/statusline-agent-bridge.sh",
    "refreshInterval": 2
  }
}
```

This is optional local configuration; Agent Bridge does not modify global Claude settings by itself.

## Worker launchers

The supervisor chooses a launcher by recipient:

- `systemd/cclocal_headless.sh` starts a non-interactive Claude Code worker. It is an example integration for a local Claude-compatible wrapper and expects the local environment variables used by that wrapper.
- `systemd/kimi_headless.sh` starts the official Kimi CLI in prompt mode and passes the task through a private request file so task text does not appear in argv or the process list.

For public or team use, review these launchers and adapt them to your own client wrappers and permission model. Do not weaken static deny rules or hide permission requests.

## Reliability model

- **One attempt barrier:** `dispatch_attempts.message_id` is the primary key. A rescan, restart or duplicate listener cannot launch the same message twice.
- **No automatic retry:** crash, timeout or uncertainty remains visible for a human decision; a short operational notice is sent without task payload.
- **Evidence-based success:** exit code 0 alone is not success. The protocol requires an acknowledged original plus the appropriate result, blocked report, follow-up or terminal conversation state.
- **Lease and timeout:** worker claims expire; a worker timeout kills only that worker process group.
- **Crash recovery:** stale attempts are reconciled by process evidence and report evidence. `--recover` examines only messages that have never been attempted.
- **Fresh database bootstrap:** the listener creates the dispatcher schema additively before scanning, so a new bridge database does not require a manual supervisor run.

## Security notes

- The bridge is local: MCP stdio, SQLite and local subprocesses.
- Do not commit `bridge.db`, runtime files, outbox reports or agent MCP configuration.
- Secrets are passed through environment variables or private files and are never written to the database, prompts, logs or progress files.
- Logs are operational metadata only. Do not redirect MCP stderr into a file that also stores secret-bearing diagnostics.
- Claude-side workers may run with permission prompts disabled. Keep their allowlist minimal and prefer an explicit `blocked` response over any uncertain action.
- Kimi-side automatic dispatch is deliberately gated off in the default configuration.
- Conversations are not authorization to perform external or irreversible actions. Deployments, pushes, purchases and messages to third parties require a human decision.

## Development

Safe tests use temporary databases and fake launchers/listeners/workers:

```bash
uv run python systemd/test_dispatcher.py
```

`test_bridge.py` is a destructive end-to-end test that deletes and recreates `bridge.db`. Run it only in a disposable copy of the project before live traffic, never against a database containing real messages.

Current safe suite: 44 dispatcher tests.

## Known limitations

See [TODO.md](TODO.md). The main open item is a SQLite WAL startup race under concurrent supervisor launches; the ordinary suite can pass while a focused concurrent stress test still exposes it.

## License

[MIT](LICENSE)

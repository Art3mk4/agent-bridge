"""agent_bridge — minimal local message-passing MCP server.

One SQLite DB (bridge.db, next to this file) is shared by per-agent server
processes: each client starts its own `server.py` over stdio with
AGENT_ID=kimi or AGENT_ID=claude. Eight tools: send_message, receive_messages,
claim_message, release_message, mark_read, start_conversation, conversation,
listener. No ports, no shell, no network. main() non-blockingly ensures one detached per-agent listener
(systemd/listener.py, flock singleton) at MCP load; see README.

Response deadlines are metadata only: no background timers, no deletion,
no automatic resend. After a deadline the caller must report the timeout
and wait for human direction; late replies remain valid.
"""

from __future__ import annotations

import functools
import importlib.util
import logging
import os
import sqlite3
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable

from mcp.server.mcpserver import MCPServer

DB_PATH = Path(__file__).resolve().parent / "bridge.db"
ERROR_LOG_PATH = Path(__file__).resolve().parent / "errors.log"
SYSTEMD_DIR = Path(__file__).resolve().parent / "systemd"
ON_MESSAGE = SYSTEMD_DIR / "on_message.py"
LISTENER_PY = SYSTEMD_DIR / "listener.py"
DISPATCH_CONFIG_PATH = SYSTEMD_DIR / "config.json"
RUNTIME_DIR = SYSTEMD_DIR / "runtime"
SCHEMA_PY = SYSTEMD_DIR / "schema.py"
AGENTS = {"kimi", "claude"}
WORKER_ID = str(uuid.uuid4())
MAX_WAIT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.25
MIN_TIMEOUT_SECONDS = 1.0
MAX_TIMEOUT_SECONDS = 3600.0
MIN_LEASE_SECONDS = 30.0
MAX_LEASE_SECONDS = 3600.0
DEFAULT_MAX_TURNS = 6
MIN_MAX_TURNS = 2
HARD_MAX_TURNS = 20
DEFAULT_CONVERSATION_TTL_SECONDS = 14400.0
MIN_CONVERSATION_TTL_SECONDS = 60.0
MAX_CONVERSATION_TTL_SECONDS = 86400.0
CONVERSATION_KINDS = {"task", "result", "notice"}


def _load_by_path(name: str, path: Path) -> Any:
    """Load a sibling module by file path (systemd/ is not a package and is
    not on sys.path when the MCP client starts this file)."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Shared dispatcher core: WAL recipe and the 0600 log handler.
_schema = _load_by_path("agent_bridge_schema", SCHEMA_PY)


def _setup_logging() -> logging.Logger:
    # Diagnostics must never reach stdout: stdout carries the MCP stdio
    # protocol framing. A plain UTF-8 file next to server.py is the only
    # sink (plus logging's own fallback to stderr, which is also safe).
    # Never log message text or other user payload values — only tool
    # names, AGENT_ID and tracebacks.
    log = logging.getLogger("agent_bridge")
    if not log.handlers:  # idempotent across re-imports / test re-runs
        # 0600 like every other diagnostic file of the bridge (tracebacks
        # carry paths and code lines, never payload, but still private).
        handler = _schema.private_file_handler(ERROR_LOG_PATH)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s agent_bridge: %(message)s")
        )
        log.addHandler(handler)
        log.propagate = False
    return log


logger = _setup_logging()


def _log_exception(event: str, exc: BaseException) -> None:
    """Log one unexpected failure: event, AGENT_ID, exception type and the
    full traceback. The exception message is deliberately dropped because
    it can echo user payload values (e.g. an int() argument) and payload
    must never reach the log; the traceback's last line still names the
    exception type. Assembled by hand rather than via
    format_exception(type, None, tb): on Python 3.10+ that call form loses
    the type and emits a stray 'NoneType: None'. stdout is never used —
    it carries the MCP stdio protocol."""
    etype = type(exc)
    tb_text = (
        "Traceback (most recent call last):\n"
        + "".join(traceback.format_tb(exc.__traceback__))
        + f"{etype.__module__}.{etype.__qualname__}"
    )
    logger.error(
        "%s agent_id=%r\n%s",
        event,
        os.environ.get("AGENT_ID"),
        tb_text,
    )


def _logged(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator for MCP tools: log unhandled Exception failures with the
    tool name, AGENT_ID and full traceback, then re-raise unchanged so the
    MCP error contract is untouched. Only Exception is caught —
    KeyboardInterrupt/SystemExit (BaseException) pass through unlogged."""

    @functools.wraps(fn)  # keeps __name__/__doc__/signature for mcp.tool()
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            _log_exception(f"tool {fn.__name__} failed", exc)
            raise

    return wrapper


mcp = MCPServer("agent_bridge")


def _connect() -> sqlite3.Connection:
    # Shared recipe (systemd/schema.py): busy_timeout first, then a WAL
    # switch retried by hand — on a not-yet-WAL file the switch fails at once
    # while another writer holds a lock, busy_timeout or not. Already-WAL
    # files (the normal case after the first start) skip the switch.
    return _schema.connect(DB_PATH)


def _add_column(con: sqlite3.Connection, name: str, decl: str) -> None:
    try:
        con.execute(f"ALTER TABLE messages ADD COLUMN {name} {decl}")
    except sqlite3.OperationalError as exc:
        # Another process added it between our check and the ALTER.
        if "duplicate column" not in str(exc).lower():
            raise


def _init_db() -> None:
    # Two servers can initialize simultaneously; a journal-mode change
    # reports SQLITE_BUSY immediately (busy_timeout is not consulted for it),
    # so the whole init is retried until the file settles in WAL mode.
    last_exc: BaseException | None = None
    for attempt in range(100):
        con = None
        try:
            con = _connect()
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    text TEXT NOT NULL,
                    reply_to INTEGER,
                    created_at REAL NOT NULL,
                    acknowledged INTEGER NOT NULL DEFAULT 0,
                    response_deadline REAL,
                    dedup_key TEXT,
                    claimed_by TEXT,
                    claim_expires_at REAL
                )
                """
            )
            # Bounded autonomous conversations between two agents: state
            # machine + audit transitions. Messages join via the
            # conversation_id column added below.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    goal TEXT NOT NULL,
                    initiator TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'active',
                    turn_count INTEGER NOT NULL DEFAULT 0,
                    max_turns INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    at REAL NOT NULL,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            # Safe in-place migration for databases created before later
            # message metadata and claim columns existed.
            columns = {row[1] for row in con.execute("PRAGMA table_info(messages)")}
            if "response_deadline" not in columns:
                _add_column(con, "response_deadline", "REAL")
            if "dedup_key" not in columns:
                _add_column(con, "dedup_key", "TEXT")
            if "claimed_by" not in columns:
                _add_column(con, "claimed_by", "TEXT")
            if "claim_expires_at" not in columns:
                _add_column(con, "claim_expires_at", "REAL")
            if "conversation_id" not in columns:
                _add_column(con, "conversation_id", "INTEGER")
            if "kind" not in columns:
                _add_column(con, "kind", "TEXT")
            if "turn" not in columns:
                _add_column(con, "turn", "INTEGER")
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_unread "
                "ON messages (recipient, acknowledged, id)"
            )
            # NULL dedup_key rows are exempt from the uniqueness constraint.
            con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup "
                "ON messages (sender, dedup_key) WHERE dedup_key IS NOT NULL"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_conversation "
                "ON messages (conversation_id, id)"
                " WHERE conversation_id IS NOT NULL"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_conv_transitions "
                "ON conversation_transitions (conversation_id, id)"
            )
            con.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            last_exc = exc
            time.sleep(0.05 * (attempt + 1))
        finally:
            if con is not None:
                con.close()
    # `exc` from the except clause is unbound here (Python 3 deletes it when
    # the clause ends), so the last error is carried explicitly.
    raise RuntimeError(
        "bridge.db stayed locked during init for >~25s") from last_exc


try:
    _init_db()
except Exception as exc:
    _log_exception("database initialization failed", exc)
    raise


def _agent() -> str:
    agent = os.environ.get("AGENT_ID")
    if agent not in AGENTS:
        raise ValueError(
            f"AGENT_ID must be one of {sorted(AGENTS)}, got {agent!r}"
        )
    return agent


# ------------------------------------------------------------- conversations


def _conversation_row(con: sqlite3.Connection, conversation_id: int):
    return con.execute(
        "SELECT id, goal, initiator, recipient, state, turn_count, max_turns,"
        " created_at, expires_at FROM conversations WHERE id = ?",
        (conversation_id,),
    ).fetchone()


def _transition(con: sqlite3.Connection, conversation_id: int, state: str,
                reason: str, now: float) -> None:
    """Move a conversation to a new state and audit the move. `reason` is a
    fixed machine token (created/expired/turns_exhausted/completed/
    needs_human/cancelled/dispatch_failed) — never message payload."""
    con.execute(
        "UPDATE conversations SET state = ?, updated_at = ? WHERE id = ?",
        (state, now, conversation_id),
    )
    con.execute(
        "INSERT INTO conversation_transitions"
        " (conversation_id, at, state, reason) VALUES (?, ?, ?, ?)",
        (conversation_id, now, state, reason),
    )


def _conversation_status(con: sqlite3.Connection, conversation_id: int,
                         now: float) -> dict[str, Any]:
    """Status view of one conversation: state machine data only — never the
    goal text, never message text (same no-payload rule as the logs)."""
    row = _conversation_row(con, conversation_id)
    if row is None:
        raise ValueError(f"conversation {conversation_id} not found")
    (cid, _goal, initiator, recipient, state, turn_count, max_turns,
     created_at, expires_at) = row
    in_flight = con.execute(
        "SELECT id, kind, sender, recipient, turn FROM messages"
        " WHERE conversation_id = ? AND acknowledged = 0"
        "   AND kind IN ('task', 'result') ORDER BY id DESC LIMIT 5",
        (cid,),
    ).fetchall()
    transitions = con.execute(
        "SELECT at, state, reason FROM conversation_transitions"
        " WHERE conversation_id = ? ORDER BY id DESC LIMIT 10",
        (cid,),
    ).fetchall()
    return {
        "conversation_id": cid,
        "state": state,
        "initiator": initiator,
        "recipient": recipient,
        "turn_count": turn_count,
        "max_turns": max_turns,
        "created_at": created_at,
        "expires_at": expires_at,
        "expired": expires_at <= now,
        "in_flight": [
            {"message_id": r[0], "kind": r[1], "sender": r[2],
             "recipient": r[3], "turn": r[4]}
            for r in in_flight
        ],
        "transitions": [
            {"at": r[0], "state": r[1], "reason": r[2]} for r in transitions
        ],
    }


def _duplicate_result(con: sqlite3.Connection, sender: str,
                      dedup_key: str) -> dict[str, Any] | None:
    row = con.execute(
        "SELECT id, response_deadline, conversation_id, kind, turn"
        " FROM messages WHERE sender = ? AND dedup_key = ?",
        (sender, dedup_key),
    ).fetchone()
    if row is None:
        return None
    out = {
        "message_id": row[0],
        "sender": sender,
        "response_deadline": row[1],
        "duplicate": True,
    }
    if row[2] is not None:
        out.update({"conversation_id": row[2], "kind": row[3], "turn": row[4]})
    return out


_CONVERSATION_INSERT = (
    "INSERT INTO messages (sender, recipient, text, reply_to, created_at,"
    " response_deadline, dedup_key, conversation_id, kind, turn)"
    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def _send_conversation_message(con: sqlite3.Connection, sender: str,
                               recipient: str, text: str,
                               reply_to: int | None, dedup_key: str | None,
                               conversation_id: int | None, kind: str,
                               now: float,
                               deadline: float) -> dict[str, Any]:
    """task/result/notice inside an existing conversation, validated and
    counted atomically: the conversation must belong to both agents, a
    task/result must answer its counterpart message in the same conversation
    and go back to its sender, the conversation must be active and unexpired,
    exactly one task/result may be in flight, and the turn counter is
    incremented in this one transaction (a send that would exceed the limit
    moves the conversation to needs_human and is rejected)."""
    if conversation_id is None:
        if reply_to is None:
            raise ValueError(
                "conversation_id is required for a conversation message "
                "(a new dialogue starts with start_conversation)")
        parent_conv = con.execute(
            "SELECT conversation_id FROM messages WHERE id = ?",
            (reply_to,),
        ).fetchone()
        if parent_conv is None or parent_conv[0] is None:
            raise ValueError("reply_to message does not belong to a conversation")
        conversation_id = parent_conv[0]
    if dedup_key is not None:
        dup = _duplicate_result(con, sender, dedup_key)
        if dup is not None:
            return dup
    con.execute("BEGIN IMMEDIATE")
    try:
        conv = _conversation_row(con, conversation_id)
        if conv is None:
            raise ValueError(f"conversation {conversation_id} not found")
        (cid, _goal, initiator, other, state, turn_count, max_turns,
         _created, expires_at) = conv
        if {sender, recipient} != {initiator, other}:
            raise ValueError(
                f"conversation {cid} belongs to {initiator} and {other}")
        parent = None
        if reply_to is not None:
            parent = con.execute(
                "SELECT sender, recipient, kind, conversation_id"
                " FROM messages WHERE id = ?",
                (reply_to,),
            ).fetchone()
            if parent is None or parent[3] != cid:
                raise ValueError(
                    "reply_to is not a message of this conversation")
        if kind == "notice":
            # Informational only: never dispatched, no turn taken, allowed
            # whatever the conversation state (a finished dialogue can still
            # receive a dispatch-error notice).
            if parent is not None and sender != parent[1]:
                raise ValueError(
                    "only the recipient of a message may notice-reply to it")
            cur = con.execute(
                _CONVERSATION_INSERT,
                (sender, recipient, text, reply_to, now, deadline, dedup_key,
                 cid, "notice", None),
            )
            con.commit()
            return {
                "message_id": cur.lastrowid,
                "conversation_id": cid,
                "kind": "notice",
                "turn": None,
                "sender": sender,
                "response_deadline": deadline,
                "duplicate": False,
            }
        if parent is None:
            raise ValueError(
                f"a conversation '{kind}' must reply_to the message it "
                "answers (a new dialogue starts with start_conversation)")
        p_sender, p_recipient, p_kind, _ = parent
        if sender != p_recipient or recipient != p_sender:
            raise ValueError(
                "conversation replies must go back to the sender of the "
                "message being answered")
        expected_parent = "task" if kind == "result" else "result"
        if p_kind != expected_parent:
            raise ValueError(f"kind '{kind}' must answer a '{expected_parent}'")
        if state != "active":
            raise ValueError(
                f"conversation {cid} is {state}: no new {kind} is allowed")
        if expires_at <= now:
            # state is 'active' here: any other state was rejected above.
            _transition(con, cid, "needs_human", "expired", now)
            con.commit()
            raise ValueError(
                f"conversation {cid} expired: moved to needs_human")
        # Exactly one message in flight. The whole answer chain — the message
        # being answered and all its ancestors — is exempt: it IS the single
        # thread of this dialogue, and its unacked members (a task released
        # after "blocked:", a result awaiting review, tasks already answered
        # down the chain) are steps of that thread, not forks. Anything else
        # unacknowledged — a duplicate result on the same task, a parallel
        # branch — is a fork and is rejected.
        exempt: list[int] = []
        seen: set[int] = set()
        cur_id = reply_to
        while cur_id is not None and cur_id not in seen:
            seen.add(cur_id)
            exempt.append(cur_id)
            prow = con.execute(
                "SELECT reply_to FROM messages WHERE id = ?", (cur_id,)
            ).fetchone()
            cur_id = prow[0] if prow is not None else None
        marks = ",".join("?" * len(exempt))
        blocker = con.execute(
            "SELECT id, kind FROM messages WHERE conversation_id = ?"
            " AND acknowledged = 0 AND kind IN ('task', 'result')"
            + (f" AND id NOT IN ({marks})" if exempt else ""),
            (cid, *exempt),
        ).fetchone()
        if blocker is not None:
            raise ValueError(
                f"conversation {cid} already has message {blocker[0]} "
                f"({blocker[1]}) in flight: exactly one at a time")
        if turn_count + 1 > max_turns:
            _transition(con, cid, "needs_human", "turns_exhausted", now)
            con.commit()
            raise ValueError(
                f"conversation {cid} reached its limit of {max_turns} turns: "
                "moved to needs_human, no more workers")
        turn = turn_count + 1
        con.execute(
            "UPDATE conversations SET turn_count = ?, updated_at = ?"
            " WHERE id = ?",
            (turn, now, cid),
        )
        try:
            cur = con.execute(
                _CONVERSATION_INSERT,
                (sender, recipient, text, reply_to, now, deadline, dedup_key,
                 cid, kind, turn),
            )
        except sqlite3.IntegrityError:
            # Lost a dedup race: the tx (turn update) must not survive.
            con.rollback()
            dup = _duplicate_result(con, sender, dedup_key)
            if dup is None:
                raise
            return dup
        con.commit()
        return {
            "message_id": cur.lastrowid,
            "conversation_id": cid,
            "kind": kind,
            "turn": turn,
            "sender": sender,
            "response_deadline": deadline,
            "duplicate": False,
        }
    except Exception:
        con.rollback()
        raise


# ----------------------------------------------------------------- listener


_LISTENER_MOD: Any = None


def _get_listener() -> Any:
    """Load systemd/listener.py once as a module (path chosen by the
    LISTENER_PY module global). Import is lazy and side-effect-free in run
    mode, so a missing listener can never break the five message tools at
    server import."""
    global _LISTENER_MOD
    if _LISTENER_MOD is None:
        _LISTENER_MOD = _load_by_path("agent_bridge_listener", LISTENER_PY)
    return _LISTENER_MOD


def _ensure_listener_at_load() -> None:
    """Auto-activation at MCP load: non-blocking, never fatal. The listener
    is detached (survives this stdio session) and deduplicated by its flock,
    so duplicate MCP sessions and headless workers create no duplicates.
    A disabled marker or AGENT_BRIDGE_AUTO_LISTEN=0 suppresses the autostart
    (see systemd/listener.py and README)."""
    try:
        agent = os.environ.get("AGENT_ID")
        if agent in AGENTS:
            _get_listener().ensure_listener(
                agent, str(DB_PATH), str(DISPATCH_CONFIG_PATH),
                str(RUNTIME_DIR), str(ON_MESSAGE))
    except Exception as exc:
        # A listener problem must not take the message tools down with it.
        _log_exception("listener auto-start failed", exc)


@mcp.tool()
@_logged
def listener(action: str) -> dict[str, Any]:
    """Manage this agent's own background listener (AGENT_ID only).

    status: enabled/running/pid and the id of the message the listener is
    currently waiting on — never task payload. progress: one human-readable
    line about the current stage and the last dispatch outcome — the exact
    same line the cclocal status line renders; ids, enums and numbers only,
    never task payload, and the call never raises. enable: removes the disabled
    marker and idempotently starts the listener (duplicate sessions create no
    duplicate — the listener holds an exclusive flock). disable: writes the
    marker; the listener then takes no new task, lets the current worker
    finish and exits. No PID is ever killed from here. Dispatch itself stays
    governed by systemd/config.json auto.<agent>.
    """
    agent = _agent()
    act = str(action).strip().lower()
    mod = _get_listener()
    if act == "status":
        return mod.status(agent, str(DISPATCH_CONFIG_PATH), str(RUNTIME_DIR))
    if act == "progress":
        try:
            line = mod.progress_line(agent, str(DISPATCH_CONFIG_PATH),
                                     str(RUNTIME_DIR), str(DB_PATH))
        except Exception:
            line = f"[{agent}] unknown"
        return {"line": line}
    if act == "enable":
        return mod.enable(agent, str(DB_PATH), str(DISPATCH_CONFIG_PATH),
                          str(RUNTIME_DIR), str(ON_MESSAGE))
    if act == "disable":
        return mod.disable(agent, str(DISPATCH_CONFIG_PATH), str(RUNTIME_DIR))
    raise ValueError(
        "action must be one of: status, progress, enable, disable")


@mcp.tool()
@_logged
def send_message(
    recipient: str,
    text: str,
    reply_to: int | None = None,
    response_timeout_seconds: float = 300,
    dedup_key: str | None = None,
    conversation_id: int | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    """Send a message to another agent; sender is this process's AGENT_ID.

    reply_to optionally links this message to the id of a previous message
    (use it to answer requests). response_timeout_seconds (clamped to
    1..3600) is persisted as response_deadline = now + timeout: pure
    metadata for the receiver — nothing is timed, deleted or resent
    automatically; after a deadline report it and wait for human direction,
    late replies stay valid. dedup_key (e.g. a task id): repeating the same
    (sender, dedup_key) returns the existing message with duplicate=true
    and inserts nothing. Returns the message id either way.

    Conversation messages belong to a bounded autonomous dialogue (see
    start_conversation): pass conversation_id and kind — 'task' (substantive
    work that wakes the recipient's listener), 'result' (report on a task;
    wakes the initiator's review worker only while the conversation is
    active) or 'notice' (informational, never wakes a worker). Omit
    conversation_id to inherit it from the conversation of reply_to.
    Conversation sends are validated atomically: the dialogue must be active
    and unexpired, the message must answer its task/result and go back to
    that message's sender, exactly one message may be in flight, and each
    task/result takes one turn (a send past the turn limit moves the
    conversation to needs_human and is rejected). A send_message without
    kind is a plain message and never becomes autonomous.
    """
    sender = _agent()
    if recipient not in AGENTS:
        raise ValueError(f"recipient must be one of {sorted(AGENTS)}")
    if recipient == sender:
        raise ValueError("recipient must differ from sender")
    timeout = max(MIN_TIMEOUT_SECONDS, min(float(response_timeout_seconds), MAX_TIMEOUT_SECONDS))
    dedup_key = dedup_key if dedup_key not in (None, "") else None
    if kind not in (None, ""):
        kind = str(kind).strip().lower()
        if kind not in CONVERSATION_KINDS:
            raise ValueError(
                f"kind must be one of {sorted(CONVERSATION_KINDS)} or omitted")
    else:
        kind = None
    if conversation_id in (None, ""):
        conversation_id = None
    elif kind is None:
        raise ValueError("kind is required when conversation_id is given")
    else:
        conversation_id = int(conversation_id)
    if reply_to is not None:
        reply_to = int(reply_to)
    now = time.time()
    deadline = now + timeout
    con = _connect()
    try:
        if kind is not None:
            return _send_conversation_message(
                con, sender, recipient, text, reply_to, dedup_key,
                conversation_id, kind, now, deadline)
        if dedup_key is not None:
            row = con.execute(
                "SELECT id, response_deadline FROM messages"
                " WHERE sender = ? AND dedup_key = ?",
                (sender, dedup_key),
            ).fetchone()
            if row is not None:
                return {
                    "message_id": row[0],
                    "sender": sender,
                    "response_deadline": row[1],
                    "duplicate": True,
                }
        try:
            cur = con.execute(
                "INSERT INTO messages"
                " (sender, recipient, text, reply_to, created_at,"
                "  response_deadline, dedup_key)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sender, recipient, text, reply_to, now, deadline, dedup_key),
            )
            con.commit()
        except sqlite3.IntegrityError:
            # Lost a race against another process with the same dedup_key.
            row = con.execute(
                "SELECT id, response_deadline FROM messages"
                " WHERE sender = ? AND dedup_key = ?",
                (sender, dedup_key),
            ).fetchone()
            if row is None:
                raise
            return {
                "message_id": row[0],
                "sender": sender,
                "response_deadline": row[1],
                "duplicate": True,
            }
        return {
            "message_id": cur.lastrowid,
            "sender": sender,
            "response_deadline": deadline,
            "duplicate": False,
        }
    finally:
        con.close()


@mcp.tool()
@_logged
def start_conversation(
    recipient: str,
    goal: str,
    max_turns: int = DEFAULT_MAX_TURNS,
    expires_in_seconds: float = DEFAULT_CONVERSATION_TTL_SECONDS,
    response_timeout_seconds: float = 300,
    dedup_key: str | None = None,
) -> dict[str, Any]:
    """Start a bounded autonomous conversation with another agent.

    Creates a conversation (goal, initiator = AGENT_ID, recipient, state
    active) and posts its root task as turn 1, so the recipient's listener
    picks it up without any manual phrase. The two agents then exchange
    task/result messages inside this conversation until one marks it
    completed or needs_human, the turn limit or expiry is reached, or it is
    cancelled. The limit counts every automatic worker turn (result reviews
    included); max_turns is clamped to 2..20 (hard cap 20) and the expiry
    window to 60..86400 s. A worker reaching the limit or a stopped state
    must not spawn new work: the dialogue goes to needs_human for a human.
    dedup_key makes repeated starts idempotent (existing root task,
    duplicate=true). An ordinary send_message never creates a conversation.
    """
    sender = _agent()
    if recipient not in AGENTS:
        raise ValueError(f"recipient must be one of {sorted(AGENTS)}")
    if recipient == sender:
        raise ValueError("recipient must differ from sender")
    if not str(goal).strip():
        raise ValueError("goal must not be empty")
    turns = max(MIN_MAX_TURNS, min(int(max_turns), HARD_MAX_TURNS))
    ttl = max(MIN_CONVERSATION_TTL_SECONDS,
              min(float(expires_in_seconds), MAX_CONVERSATION_TTL_SECONDS))
    timeout = max(MIN_TIMEOUT_SECONDS, min(float(response_timeout_seconds), MAX_TIMEOUT_SECONDS))
    dedup_key = dedup_key if dedup_key not in (None, "") else None
    now = time.time()
    deadline = now + timeout
    expires_at = now + ttl
    con = _connect()
    try:
        if dedup_key is not None:
            dup = _duplicate_result(con, sender, dedup_key)
            if dup is not None:
                return dup
        con.execute("BEGIN IMMEDIATE")
        try:
            cur = con.execute(
                "INSERT INTO conversations (goal, initiator, recipient, state,"
                " turn_count, max_turns, created_at, expires_at, updated_at)"
                " VALUES (?, ?, ?, 'active', 1, ?, ?, ?, ?)",
                (goal, sender, recipient, turns, now, expires_at, now),
            )
            cid = cur.lastrowid
            con.execute(
                "INSERT INTO conversation_transitions"
                " (conversation_id, at, state, reason)"
                " VALUES (?, ?, 'active', 'created')",
                (cid, now),
            )
            cur = con.execute(
                _CONVERSATION_INSERT,
                (sender, recipient, goal, None, now, deadline, dedup_key,
                 cid, "task", 1),
            )
            message_id = cur.lastrowid
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            dup = _duplicate_result(con, sender, dedup_key)
            if dup is None:
                raise
            return dup
        return {
            "conversation_id": cid,
            "message_id": message_id,
            "kind": "task",
            "turn": 1,
            "max_turns": turns,
            "state": "active",
            "expires_at": expires_at,
            "response_deadline": deadline,
            "duplicate": False,
        }
    finally:
        con.close()


@mcp.tool()
@_logged
def conversation(action: str, conversation_id: int) -> dict[str, Any]:
    """Inspect or steer a conversation only its two agents belong to.

    status — state, turn_count/max_turns, expiry, recent transitions and the
    in-flight message; never the goal text, never message text (no payload,
    no secrets). cancel — stop all further automatic dispatch for it (side
    effects already produced are NOT rolled back); allowed from
    active/blocked/needs_human. complete — the initial goal is verifiably
    achieved (the review worker's outcome a; only from active). needs_human —
    a human decision is required (outcome c; only from active). Repeating
    the call for the state the conversation is already in is an idempotent
    no-op; a different terminal state is rejected, so the two agents can
    never fight over the outcome.
    """
    agent = _agent()
    cid = int(conversation_id)
    act = str(action).strip().lower()
    if act not in {"status", "cancel", "complete", "needs_human"}:
        raise ValueError(
            "action must be one of: status, cancel, complete, needs_human")
    now = time.time()
    con = _connect()
    try:
        if act == "status":
            conv = _conversation_row(con, cid)
            if conv is None:
                raise ValueError(f"conversation {cid} not found")
            if agent not in (conv[2], conv[3]):
                raise ValueError(
                    f"conversation {cid} belongs to {conv[2]} and {conv[3]}")
            return _conversation_status(con, cid, now)
        con.execute("BEGIN IMMEDIATE")
        try:
            conv = _conversation_row(con, cid)
            if conv is None:
                raise ValueError(f"conversation {cid} not found")
            if agent not in (conv[2], conv[3]):
                raise ValueError(
                    f"conversation {cid} belongs to {conv[2]} and {conv[3]}")
            state = conv[4]
            target = {"cancel": "cancelled", "complete": "completed",
                      "needs_human": "needs_human"}[act]
            if state == target:
                con.rollback()  # idempotent no-op
                return _conversation_status(con, cid, now)
            allowed = {
                "cancel": {"active", "blocked", "needs_human"},
                "complete": {"active"},
                "needs_human": {"active"},
            }[act]
            if state not in allowed:
                raise ValueError(f"cannot {act} a {state} conversation")
            _transition(con, cid, target, target, now)
            con.commit()
        except Exception:
            con.rollback()
            raise
        return _conversation_status(con, cid, now)
    finally:
        con.close()


def _message_payload(row: tuple[Any, ...], now: float) -> dict[str, Any]:
    return {
        "id": row[0],
        "sender": row[1],
        "text": row[2],
        "reply_to": row[3],
        "created_at": row[4],
        "response_deadline": row[5],
        "claim_expires_at": row[6],
        "owned_by_current_worker": bool(
            row[7] == WORKER_ID and row[6] is not None and row[6] > now
        ),
        "conversation_id": row[8],
        "kind": row[9],
        "turn": row[10],
    }


_MESSAGE_COLUMNS = (
    "SELECT id, sender, text, reply_to, created_at, response_deadline,"
    " claim_expires_at, claimed_by, conversation_id, kind, turn FROM messages"
)


@mcp.tool()
@_logged
def claim_message(message_id: int, lease_seconds: float = 300) -> dict[str, Any]:
    """Atomically claim an unread message for this MCP server process.

    An active claim by another process cannot be taken over, acknowledged or
    released. Repeating the call as the current owner renews the lease. An
    expired claim can be taken by any process belonging to the recipient.
    lease_seconds is clamped to 30..3600; expiry is checked during tool calls,
    without background timers.
    """
    agent = _agent()
    message_id = int(message_id)
    lease = max(MIN_LEASE_SECONDS, min(float(lease_seconds), MAX_LEASE_SECONDS))
    now = time.time()
    expires_at = now + lease
    con = _connect()
    try:
        cur = con.execute(
            "UPDATE messages SET claimed_by = ?, claim_expires_at = ?"
            " WHERE id = ? AND recipient = ? AND acknowledged = 0"
            " AND (claim_expires_at IS NULL OR claim_expires_at <= ?"
            "      OR claimed_by = ?)",
            (WORKER_ID, expires_at, message_id, agent, now, WORKER_ID),
        )
        con.commit()
        if cur.rowcount == 1:
            row = con.execute(
                f"{_MESSAGE_COLUMNS} WHERE id = ?",
                (message_id,),
            ).fetchone()
            return {
                "message_id": message_id,
                "status": "claimed",
                "claimed": True,
                "claim_expires_at": expires_at,
                "message": _message_payload(row, now),
            }

        row = con.execute(
            "SELECT acknowledged FROM messages WHERE id = ? AND recipient = ?",
            (message_id, agent),
        ).fetchone()
        status = "not_found"
        if row is not None:
            status = "acknowledged" if row[0] else "claimed_by_other"
        return {
            "message_id": message_id,
            "status": status,
            "claimed": False,
            "claim_expires_at": None,
        }
    finally:
        con.close()


@mcp.tool()
@_logged
def release_message(message_id: int) -> dict[str, Any]:
    """Release this process's active claim without acknowledging the message."""
    agent = _agent()
    message_id = int(message_id)
    now = time.time()
    con = _connect()
    try:
        cur = con.execute(
            "UPDATE messages SET claimed_by = NULL, claim_expires_at = NULL"
            " WHERE id = ? AND recipient = ? AND acknowledged = 0"
            " AND claimed_by = ? AND claim_expires_at > ?",
            (message_id, agent, WORKER_ID, now),
        )
        con.commit()
        if cur.rowcount == 1:
            return {"message_id": message_id, "status": "released", "released": True}

        row = con.execute(
            "SELECT acknowledged, claimed_by, claim_expires_at FROM messages"
            " WHERE id = ? AND recipient = ?",
            (message_id, agent),
        ).fetchone()
        status = "not_found"
        if row is not None:
            if row[0]:
                status = "acknowledged"
            elif row[2] is None or row[2] <= now:
                status = "not_claimed"
            else:
                status = "claimed_by_other"
        return {"message_id": message_id, "status": status, "released": False}
    finally:
        con.close()


@mcp.tool()
@_logged
def receive_messages(limit: int = 20, wait_seconds: float = 0) -> dict[str, Any]:
    """Return available unread messages addressed to AGENT_ID, oldest first.

    Messages actively claimed by another server process are hidden. Waits up
    to wait_seconds, capped at MAX_WAIT_SECONDS (30). Messages stay unread
    until mark_read. Each message carries response_deadline, claim_expires_at
    and owned_by_current_worker; cursor is the last returned id, or None.
    """
    agent = _agent()
    limit = max(1, min(int(limit), 100))
    wait = max(0.0, min(float(wait_seconds), MAX_WAIT_SECONDS))
    deadline = time.monotonic() + wait
    rows = []
    now = time.time()
    while True:
        now = time.time()
        con = _connect()
        try:
            rows = con.execute(
                f"{_MESSAGE_COLUMNS}"
                " WHERE recipient = ? AND acknowledged = 0"
                " AND (claim_expires_at IS NULL OR claim_expires_at <= ?"
                "      OR claimed_by = ?)"
                " ORDER BY id LIMIT ?",
                (agent, now, WORKER_ID, limit),
            ).fetchall()
        finally:
            con.close()
        if rows or time.monotonic() >= deadline:
            break
        time.sleep(POLL_INTERVAL_SECONDS)
    messages = [_message_payload(row, now) for row in rows]
    return {
        "messages": messages,
        "cursor": messages[-1]["id"] if messages else None,
    }


@mcp.tool()
@_logged
def mark_read(message_ids: list[int]) -> dict[str, Any]:
    """Acknowledge messages by id; only their recipient (AGENT_ID) can do so.

    Already-acknowledged, foreign and actively claimed-by-other-worker ids are
    skipped silently. Free messages and expired claims remain compatible with
    the original recipient-only acknowledgement behavior.
    """
    agent = _agent()
    ids = [int(i) for i in message_ids]
    if not ids:
        return {"acknowledged": 0}
    now = time.time()
    con = _connect()
    try:
        cur = con.executemany(
            "UPDATE messages SET acknowledged = 1"
            " WHERE id = ? AND recipient = ?"
            " AND (claim_expires_at IS NULL OR claim_expires_at <= ?"
            "      OR claimed_by = ?)",
            [(i, agent, now, WORKER_ID) for i in ids],
        )
        con.commit()
        return {"acknowledged": cur.rowcount}
    finally:
        con.close()


def main() -> None:
    try:
        _ensure_listener_at_load()
        mcp.run(transport="stdio")
    except Exception as exc:
        # Unexpected server failure: record it, then let the process die as
        # before. KeyboardInterrupt/SystemExit are BaseException and pass
        # through unlogged.
        _log_exception("main() exited with an unexpected failure", exc)
        raise


if __name__ == "__main__":
    main()

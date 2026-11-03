#!/usr/bin/env python3
"""Shared dispatcher core for agent_bridge — the one place for the rules that
server.py, listener.py and on_message.py must agree on byte for byte.

What lives here:

* the dispatcher DDL (`dispatch_attempts`) and its additive migration;
* `connect()` / `ensure_wal()` — the connection recipe every process uses;
* `load_dispatch_config()` — the single reader of systemd/config.json;
* `find_candidates()` — the single definition of "which message may wake a
  worker". The listener picks the first one for its agent, `--recover` walks
  all of them, and on_message re-checks the same rules for one id, so the
  three can no longer drift apart;
* `private_file_handler()` — a logging handler whose file is 0600 from birth.

Why the WAL switch is retried by hand (P0-1): while a database is still in a
rollback journal mode, `PRAGMA journal_mode=WAL` fails *immediately* with
"database is locked" when another connection holds a RESERVED lock —
busy_timeout is not consulted for that request. Once the file is in WAL the
pragma is a cheap no-op, so the switch is skipped when the mode is already
WAL and otherwise retried with a growing sleep.

Deadlines vs. age (P1-8): `response_deadline` is pure metadata for the worker
and the human, exactly as send_message documents. It no longer decides
whether a never-attempted message is dispatched — a message that arrived
while the listener was stopped, restarting or busy is delivered when it comes
back. What does bound automatic dispatch is the message AGE
(`dispatch_max_age_seconds` in config.json, default 24 h): an old backlog
found after a long outage is never fired automatically, it waits for a human
(`listener.py pending` lists it, a manual `on_message.py <id>` runs it).

How to load this file: NOT `import schema`. listener.py is itself loaded by
server.py through importlib.util.spec_from_file_location, and under that
mechanism this directory is not on sys.path — a plain import would raise
ModuleNotFoundError inside the MCP process. Every user loads it by path.

Pure stdlib, no side effects at import.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

AGENTS = ("claude", "kimi")
DEFAULT_AUTO = {"claude": True, "kimi": False}
DEFAULT_MAX_AGE_SECONDS = 86400.0
BUSY_TIMEOUT_MS = 5000

# Byte-for-byte the table on_message.py used to create inline; additive only.
DISPATCH_ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS dispatch_attempts (
    message_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    pid INTEGER,
    exit_code INTEGER,
    error_kind TEXT
)
"""


def _is_locked(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and \
        "locked" in str(exc).lower()


# ---------------------------------------------------------------- connection

def ensure_wal(con: sqlite3.Connection, retries: int = 100) -> None:
    """Put the database into WAL mode, tolerating a concurrent writer.

    Already-WAL files return at once (reading the mode takes no write lock).
    Otherwise the switch is retried while it reports "locked" — see the
    module docstring for why busy_timeout does not cover this case.
    """
    row = con.execute("PRAGMA journal_mode").fetchone()
    if row and str(row[0]).lower() == "wal":
        return
    for attempt in range(retries):
        try:
            con.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:
            if not _is_locked(exc):
                raise
            time.sleep(min(0.05 * (attempt + 1), 1.0))
    raise RuntimeError("bridge.db stayed locked while switching to WAL")


def connect(db: str | Path, *, wal: bool = True,
            retries: int = 100) -> sqlite3.Connection:
    """The connection recipe shared by every process: busy_timeout first
    (it must be in place before anything takes a lock), then WAL."""
    con = sqlite3.connect(str(db), timeout=BUSY_TIMEOUT_MS / 1000)
    try:
        con.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        if wal:
            ensure_wal(con, retries)
    except BaseException:
        con.close()
        raise
    return con


def connect_readonly(db: str | Path,
                     timeout: float = 1.0) -> sqlite3.Connection | None:
    """Read-only connection, or None when the file does not exist (sqlite
    would otherwise create an empty stray database on connect)."""
    p = Path(db)
    if not p.exists():
        return None
    con = sqlite3.connect(p.absolute().as_uri() + "?mode=ro", uri=True,
                          timeout=timeout)
    con.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    return con


# ------------------------------------------------------------------- schema

def is_bridge_db(con: sqlite3.Connection) -> bool:
    """True when this connection points at a real bridge database."""
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages'"
    ).fetchone() is not None


def has_dispatch_table(con: sqlite3.Connection) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table'"
        " AND name='dispatch_attempts'"
    ).fetchone() is not None


def has_conversations(con: sqlite3.Connection) -> bool:
    """True when the additive conversations layer exists in this DB."""
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table'"
                   " AND name='conversations'").fetchone() is None:
        return False
    return any(r[1] == "kind" for r in
               con.execute("PRAGMA table_info(messages)"))


def ensure_dispatch_schema(con: sqlite3.Connection, retries: int = 100) -> bool:
    """Create dispatch_attempts if absent; return True when it exists after
    the call. Returns False (creating nothing) when the file is not a bridge
    database at all — sqlite3.connect() creates an empty file for a mistyped
    path, and "messages is missing" is the only reliable proof of that.

    The DDL is strictly additive and idempotent, which is what makes it safe
    against a live bridge.db. BUSY is retried with a growing sleep;
    RuntimeError once the budget is spent (a locked live database must not
    silently degrade into 'nothing to do').
    """
    if not is_bridge_db(con):
        return False
    for attempt in range(retries):
        try:
            con.execute(DISPATCH_ATTEMPTS_DDL)
            con.commit()
            return True
        except sqlite3.OperationalError as exc:
            if not _is_locked(exc):
                raise
            time.sleep(min(0.05 * (attempt + 1), 1.0))
    raise RuntimeError("bridge.db stayed locked during dispatch schema init")


# ------------------------------------------------------------------- config

def load_dispatch_config(path: str | Path) -> tuple[dict[str, Any], str | None]:
    """Read systemd/config.json. Returns (config, error_kind).

    config = {"auto": {agent: bool}, "max_age_seconds": float}. A missing
    file means defaults (claude automated, kimi not). A malformed file
    automates nothing — fail closed — and error_kind names the exception type
    (never the content, which could be anything). A missing or invalid
    dispatch_max_age_seconds falls back to the default.
    """
    cfg: dict[str, Any] = {"auto": dict(DEFAULT_AUTO),
                           "max_age_seconds": DEFAULT_MAX_AGE_SECONDS}
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return cfg, None
    except Exception as exc:  # noqa: BLE001 — any parse problem fails closed
        cfg["auto"] = {a: False for a in AGENTS}
        return cfg, type(exc).__name__
    if not isinstance(raw, dict):
        cfg["auto"] = {a: False for a in AGENTS}
        return cfg, "NotAnObject"
    auto = raw.get("auto", DEFAULT_AUTO)
    if not isinstance(auto, dict):
        cfg["auto"] = {a: False for a in AGENTS}
        return cfg, "BadAuto"
    cfg["auto"] = {a: bool(auto.get(a, False)) for a in AGENTS}
    age = raw.get("dispatch_max_age_seconds")
    if isinstance(age, (int, float)) and not isinstance(age, bool) and age > 0:
        cfg["max_age_seconds"] = float(age)
    return cfg, None


def auto_enabled(path: str | Path, agent: str) -> bool:
    cfg, _err = load_dispatch_config(path)
    return bool(cfg["auto"].get(agent, False))


# -------------------------------------------------------------- dispatch set

_ROOT_ONLY = "m.reply_to IS NULL"
# (a) legacy root tasks — an autonomous conversation is never retroactive;
# (b) 'task'/'result' of a still-active, unexpired conversation. Notices never
# wake anything, and a malformed link (kind NULL with a conversation id, a
# foreign id) matches neither branch.
_ROOT_OR_LIVE_CONVERSATION = (
    "((m.conversation_id IS NULL AND m.kind IS NULL AND m.reply_to IS NULL)"
    " OR (m.conversation_id IS NOT NULL AND m.kind IN ('task','result')"
    "     AND c.state = 'active' AND c.expires_at > :now))"
)


def candidate_query(conversations: bool, n_agents: int,
                    with_age: bool = True) -> str:
    """SQL for never-attempted dispatchable messages. Named parameters:
    :now, :min_created (only when with_age) and :a0.. for the recipients."""
    agents = ", ".join(f":a{i}" for i in range(n_agents))
    return (
        "SELECT m.id, m.recipient, m.created_at FROM messages m"
        + (" LEFT JOIN conversations c ON c.id = m.conversation_id"
           if conversations else "")
        + f" WHERE m.recipient IN ({agents}) AND m.acknowledged = 0"
        + ("   AND m.created_at >= :min_created" if with_age else "")
        + "   AND (m.claim_expires_at IS NULL OR m.claim_expires_at <= :now)"
        + "   AND " + (_ROOT_OR_LIVE_CONVERSATION if conversations
                       else _ROOT_ONLY)
        + "   AND NOT EXISTS (SELECT 1 FROM dispatch_attempts a"
          "                   WHERE a.message_id = m.id)"
        + " ORDER BY m.id"
    )


def find_candidates(con: sqlite3.Connection, agents: Iterable[str],
                    now: float, max_age: float | None,
                    limit: int | None = None) -> list[tuple[int, str, float]]:
    """(id, recipient, created_at) of every never-attempted message that may
    wake a worker, oldest first. max_age=None ignores the age bound (used
    only by read-only reporting). The caller has checked that
    dispatch_attempts exists."""
    agents = list(agents)
    if not agents:
        return []
    params: dict[str, Any] = {"now": now}
    params.update({f"a{i}": a for i, a in enumerate(agents)})
    if max_age is not None:
        params["min_created"] = now - max_age
    sql = candidate_query(has_conversations(con), len(agents),
                          with_age=max_age is not None)
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return [(int(r[0]), str(r[1]), float(r[2]))
            for r in con.execute(sql, params).fetchall()]


# ------------------------------------------------------------------ logging

class PrivateFileHandler(logging.FileHandler):
    """logging.FileHandler whose file is created 0600 instead of by umask; an
    existing file (e.g. a legacy 0644 one) is pressed down to 0600 on open."""

    def _open(self):  # type: ignore[override]
        fd = os.open(self.baseFilename,
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.chmod(self.baseFilename, 0o600)
        except OSError:
            pass
        return os.fdopen(fd, "a", encoding=self.encoding,
                         errors=self.errors)


def private_file_handler(path: str | Path) -> logging.FileHandler:
    return PrivateFileHandler(str(path), encoding="utf-8")

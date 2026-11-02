#!/usr/bin/env python3
"""Canonical dispatcher schema for agent_bridge — the one migration path.

`dispatch_attempts` is owned by the dispatcher (this project), not by the MCP
message contract in server.py. Historically only on_message.py created it,
which made a fresh database undispatchable: the listener's scan refused to
look at anything until the table existed, and the process that creates the
table only ever runs after a successful scan. So the DDL lives here and BOTH
entry points call it — on_message.py before it validates, listener.py once
before its first scan.

The migration is strictly additive and idempotent (CREATE TABLE IF NOT EXISTS,
nothing dropped, no ALTER, no data touched), which is what makes it safe
against a live bridge.db.

Not-a-database guard: sqlite3.connect() CREATES an empty file for a mistyped
path, so "the messages table is missing" is the only reliable proof that we
are looking at a bridge database. Without that check a wrong path would turn
today's quiet "nothing to do" into a permanent OperationalError storm.

Pure stdlib, no side effects at import, no connection of its own: the caller
passes a connection it configured itself (server.py/on_message.py set
busy_timeout and WAL; this module never touches pragmas).

How to load it: NOT `import schema`. listener.py is itself loaded by
server.py through importlib.util.spec_from_file_location, and under that
mechanism this directory is not on sys.path — a plain import would raise
ModuleNotFoundError inside the MCP process and take the `listener` tool and
the autostart down with it. Both files therefore use the same
spec_from_file_location loader server.py already uses.
"""

from __future__ import annotations

import sqlite3
import time

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


def is_bridge_db(con: sqlite3.Connection) -> bool:
    """True when this connection points at a real bridge database."""
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages'"
    ).fetchone() is not None


def ensure_dispatch_schema(con: sqlite3.Connection, retries: int = 100) -> bool:
    """Create dispatch_attempts if absent; return True when it exists after
    the call. Returns False (creating nothing) when the file is not a bridge
    database at all — the caller then behaves exactly as it did before this
    module existed: nothing is dispatchable.

    BUSY is retried the same way server._init_db and the old inline
    on_message._init_schema did: only for a lock error, with a growing sleep,
    and RuntimeError once the budget is spent (a locked live database must not
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
            if "locked" not in str(exc).lower():
                raise
            time.sleep(0.05 * (attempt + 1))
    raise RuntimeError("bridge.db stayed locked during dispatch schema init")

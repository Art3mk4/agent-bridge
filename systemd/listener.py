#!/usr/bin/env python3
"""Per-agent MCP listener for agent_bridge.

One independent detached process per AGENT_ID (`run --agent <id>`), auto-started
by server.py main() at MCP load (survives the CLI/MCP exit, start_new_session).
Every ~1 s it scans bridge.db for ONE dispatchable message for its agent — a
legacy root task (reply_to IS NULL) or a 'task'/'result' of a still-active
unexpired conversation (notices never wake a worker) — and runs the existing
per-message supervisor systemd/on_message.py for it, waiting for the
supervisor to exit before scanning again. Before the first scan it additively
ensures the dispatcher's own table (systemd/schema.py) in an existing
bridge.db: a fresh database has no dispatch_attempts until some supervisor
runs, and a supervisor only ever runs after a successful scan, so without that
step no message could ever be dispatched. Expired active conversations are
moved to needs_human here (idempotent, no payload). All worker lifecycle logic — claim,
dispatch_attempts row, timeout, evidence classification, notices, no-retry —
lives in on_message.py; this file only decides when it is woken and reports
which stage it is at. There is deliberately no second
claim/result/retry implementation here.

Progress: the listener alone writes runtime/<agent>.progress.json (atomic
replace, ids/enums/numbers only) with the current stage — received →
supervising → finished — and the outcome of the last dispatch. The verdict
itself is READ from dispatch_attempts, never recomputed here, and a reader
only trusts the file when its listener_pid matches the /proc-verified pid, so
status can report "unknown" but never an invented stage.

Singleton: an exclusive non-blocking flock on runtime/<agent>.listener.lock.
Duplicate MCP sessions or headless workers spawn no duplicate listener — the
loser exits 0 immediately.

enable/disable is a marker file, never a signal, and no PID is ever killed
from here: `disable` creates runtime/<agent>.disabled; the running listener
notices within ~1 s, takes no new task, lets the current worker finish and
exits. A marker (or AGENT_BRIDGE_AUTO_LISTEN=0 in the MCP environment) also
prevents the autostart on the next MCP load. `enable` removes the marker and
idempotently starts the listener.

The recipient's automation gate (systemd/config.json `auto.<agent>`) is
re-read on every scan; while it is false the listener idles. Without this gate
here, on_message's own config skip (which creates no attempt row) would make
the scan re-fire every second.

Payload is never logged and never appears in argv: only message ids, states,
exit codes and pids. Logs go to runtime/dispatcher.log (same file as the
supervisor's), rotated once at startup when over LOG_MAX_BYTES.

CLI (emergency control without an MCP session):
    listener.py run    --agent kimi|claude [--db --config --runtime-dir --on-message]
    listener.py status --agent ... [--config --runtime-dir]
    listener.py enable --agent ... [--db --config --runtime-dir --on-message]
    listener.py disable --agent ... [--config --runtime-dir]
    listener.py progress --agent ... [--db --config --runtime-dir]
                                                                # human one-liner,
                                                                # exit 0 always
    listener.py watch    --agent ... [--db --once]              # 1 s refresh
    listener.py pending  --agent ... [--db --config]            # read-only
                                                                # backlog JSON
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent          # agent_bridge/systemd
ROOT = HERE.parent                              # agent_bridge

DEFAULT_DB = ROOT / "bridge.db"
DEFAULT_CONFIG = HERE / "config.json"
DEFAULT_RUNTIME = HERE / "runtime"
DEFAULT_ON_MESSAGE = HERE / "on_message.py"

AGENTS = {"kimi", "claude"}

POLL_SECONDS = 1.0
SLEEP_SLICE = 0.2
MAX_BACKOFF_SECONDS = 60.0
# on_message self-limits workers at 3300 s; this is only the watchdog for a
# wedged supervisor. Crossing it kills only the supervisor's own process
# group (it was started with start_new_session), never anything unverified.
WORKER_SAFETY_TIMEOUT = 3600.0
LOG_MAX_BYTES = 1_000_000
# Small budget on purpose: on_message keeps the full 100 retries, while a
# listener startup blocked for minutes on a locked live bridge.db would look
# like a dead listener. Failure here only defers the first dispatch.
SCHEMA_RETRIES = 10
PROGRESS_REFRESH_SECONDS = 1.0
# A live pid with no progress file: inside this window after its start the
# listener is still between the pidfile write and the tracker's first flush
# ("starting"); past it, it is too old to write the file at all ("legacy").
STARTING_GRACE_SECONDS = 60.0
# Written first into every progress document; a document without it comes
# from a listener that predates the marker, which status() calls "legacy".
PROGRESS_FORMAT_VERSION = 2


# ------------------------------------------------------------- runtime files

def _runtime(runtime_dir: str | Path) -> Path:
    rt = Path(runtime_dir)
    rt.mkdir(parents=True, exist_ok=True)
    try:
        rt.chmod(0o700)
    except OSError:
        pass
    return rt


def _lock_path(rt: Path, agent: str) -> Path:
    return rt / f"{agent}.listener.lock"


def _pid_path(rt: Path, agent: str) -> Path:
    return rt / f"{agent}.listener.pid"


def _state_path(rt: Path, agent: str) -> Path:
    return rt / f"{agent}.listener.state"


def _marker_path(rt: Path, agent: str) -> Path:
    return rt / f"{agent}.disabled"


def _write_private(path: Path, text: str) -> None:
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, text.encode("utf-8"))
    finally:
        os.close(fd)


def _log(runtime_dir: Path, msg: str) -> None:
    # Metadata only: ids, states, pids, exit codes. Never payload.
    # os.open with 0o600 (same pattern as _write_private): plain open("a")
    # creates the file by umask (0644). The mode applies only at creation,
    # so an existing log is chmod'ed too — a pre-fix 0644 file gets pressed
    # down to 0600 on the next write.
    path = Path(runtime_dir) / "dispatcher.log"
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, (f"{time.strftime('%Y-%m-%d %H:%M:%S')} INFO "
                          f"listener: {msg}\n").encode("utf-8"))
        finally:
            os.close(fd)
    except OSError:
        return
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _rotate_log(rt: Path) -> None:
    try:
        p = rt / "dispatcher.log"
        if p.stat().st_size > LOG_MAX_BYTES:
            os.replace(p, rt / "dispatcher.log.1")
            # os.replace keeps the old mode; the rotated copy must not stay
            # world-readable while the fresh log is recreated by _log.
            (rt / "dispatcher.log.1").chmod(0o600)
    except OSError:
        pass


def _set_state(rt: Path, agent: str, state: str) -> None:
    try:
        _write_private(_state_path(rt, agent), state + "\n")
    except OSError:
        pass


def _listener_pid(rt: Path, agent: str) -> int | None:
    """Pid from the pidfile, only if /proc proves it is a live listener
    process for this very agent (guards against recycled pids; a zombie has
    an empty cmdline and fails the check too)."""
    try:
        pid = int(_pid_path(rt, agent).read_text().strip())
    except (OSError, ValueError):
        return None
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    args = raw.split(b"\0")
    if not any(a.endswith(b"listener.py") for a in args):
        return None
    strs = [a.decode("utf-8", "replace") for a in args]
    try:
        i = strs.index("--agent")
    except ValueError:
        return None
    if i + 1 < len(strs) and strs[i + 1] == agent:
        return pid
    return None


# ------------------------------------------------------------------- gates

def _config_enabled(config_path: str | Path, agent: str) -> bool:
    # One reader for listener and on_message (schema.load_dispatch_config):
    # missing file -> defaults, malformed -> nothing automated (fail closed).
    return _schema_mod().auto_enabled(config_path, agent)


def _max_age(config_path: str | Path) -> float:
    cfg, _err = _schema_mod().load_dispatch_config(config_path)
    return cfg["max_age_seconds"]


def _is_disabled(rt: Path, agent: str) -> bool:
    return _marker_path(rt, agent).exists()


# ----------------------------------------------------------- dispatch schema

_SCHEMA_MOD: Any = None


def _schema_mod() -> Any:
    """Load systemd/schema.py by path, never by name.

    server.py loads THIS file through importlib.util.spec_from_file_location,
    and under that mechanism systemd/ is not on sys.path — a plain
    `import schema` here would raise ModuleNotFoundError inside the MCP
    process and take the `listener` tool and the autostart down with it. Same
    loader server.py uses for listener.py itself.
    """
    global _SCHEMA_MOD
    if _SCHEMA_MOD is None:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "agent_bridge_schema", HERE / "schema.py")
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load schema module from {HERE / 'schema.py'}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _SCHEMA_MOD = mod
    return _SCHEMA_MOD


def _ensure_schema(agent: str, db: str | Path, rt: Path) -> None:
    """Additively ensure dispatch_attempts once, before the first scan.

    Without this a fresh bridge.db is undispatchable: _scan returns None while
    the table is missing, and the supervisor that creates it only ever runs
    after a successful scan. Strictly best-effort — any failure only defers
    the table to the supervisor's own full-retry attempt and must never stop
    the listener. A missing file is skipped outright so a mistyped --db cannot
    leave an empty stray database behind, and schema.py itself creates nothing
    in a file that is not a bridge database.
    """
    if not Path(db).exists():
        _log(rt, f"agent={agent} dispatch schema skipped: no database at --db")
        return
    con = None
    try:
        sm = _schema_mod()
        con = sm.connect(db, wal=False)
        had = sm.has_dispatch_table(con)
        if sm.ensure_dispatch_schema(con, retries=SCHEMA_RETRIES) and not had:
            _log(rt, f"agent={agent} dispatch_attempts created")
    except Exception as exc:
        # Kind only: locked vs corrupt vs no-permission, without leaking paths.
        _log(rt, f"agent={agent} dispatch schema deferred "
                 f"kind={type(exc).__name__}")
    finally:
        if con is not None:
            con.close()


# ------------------------------------------------------------------- progress

def _progress_path(rt: Path, agent: str) -> Path:
    return rt / f"{agent}.progress.json"


def _load_progress(rt: Path, agent: str) -> tuple[dict[str, Any] | None, str]:
    """The progress document and why it is not there: ("ok", doc),
    ("missing", None), ("unreadable", None). The difference is the whole
    point — a file that cannot be read is a fault, a file that is absent is
    either a starting listener or an old one, and only status() holding the
    live pid can say which."""
    path = _progress_path(rt, agent)
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "missing"
    except (OSError, ValueError):
        return None, "unreadable"
    if not isinstance(doc, dict):
        return None, "unreadable"
    return doc, "ok"


def _read_progress(rt: Path, agent: str) -> dict[str, Any] | None:
    doc, state = _load_progress(rt, agent)
    return doc if state == "ok" else None


def _process_age_seconds(pid: int) -> float | None:
    """How long the process has been alive, from /proc. None means
    "unverifiable" and
    every caller must read that as "not young". Parsing lives in
    schema.proc_info, shared with the supervisor's pid check."""
    info = _schema_mod().proc_info(pid)
    return None if info is None else info[1]


class _ProgressTracker:
    """Writer side of runtime/<agent>.progress.json.

    Only the listener holding the flock writes it; each write is atomic (tmp
    file in the same directory + os.replace) so a concurrent reader never sees
    a half-written document; and the content is ids, stage names and numbers —
    never task text, prompts or worker output.

    A reader cannot tell a stale document from a live one by mtime, so every
    write carries listener_pid and status()/progress_line() compare it against
    the /proc-verified pidfile before believing a single stage. The stages are
    exactly received → supervising → finished: the pid obtained after Popen is
    the supervisor's (the headless worker lives only in dispatch_attempts.pid),
    so no stage here claims to see the worker process.
    """

    def __init__(self, rt: Path, agent: str, pid: int):
        self.path = _progress_path(rt, agent)
        prev = _read_progress(rt, agent) or {}
        self.doc: dict[str, Any] = {"format_version": PROGRESS_FORMAT_VERSION,
                                    "listener_pid": pid, "updated_at": None,
                                    "current": None, "last": None}
        leftover = prev.get("current")
        if isinstance(leftover, dict) and leftover.get("message") is not None:
            # The previous listener died mid-dispatch (kill, crash, reboot).
            # "interrupted" is the listener's word about its own process, not
            # a verdict on the task — dispatch_attempts and on_message
            # --recover still hold the real outcome. Idempotent: the next
            # start sees current None and changes nothing.
            self.doc["last"] = {"message": leftover["message"],
                                "stage": "finished", "result": "interrupted",
                                "supervisor_rc": None, "duration_s": None}
        self._flush()

    def _flush(self) -> None:
        self.doc["updated_at"] = time.time()
        tmp = self.path.with_name(self.path.name + ".tmp")
        try:
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, json.dumps(self.doc, ensure_ascii=False)
                         .encode("utf-8"))
            finally:
                os.close(fd)
            os.replace(tmp, self.path)
        except OSError:
            pass  # progress is an observation, never a reason to stop working

    def received(self, mid: int) -> None:
        self.doc["current"] = {"message": mid, "stage": "received",
                               "supervisor_pid": None, "since": time.time()}
        self._flush()

    def supervising(self, mid: int, sup_pid: int) -> None:
        cur = self.doc.get("current") or {"message": mid, "stage": "supervising",
                                          "since": time.time()}
        cur["message"] = mid
        cur["stage"] = "supervising"
        cur["supervisor_pid"] = sup_pid
        self.doc["current"] = cur
        self._flush()

    def finished(self, mid: int, result: str | None, rc: int | None,
                 duration_s: float | None) -> None:
        last: dict[str, Any] = {"message": mid, "stage": "finished",
                                "supervisor_rc": rc,
                                "duration_s": round(duration_s, 1)
                                if duration_s is not None else None}
        if result is not None:
            last["result"] = result
        self.doc["current"] = None
        self.doc["last"] = last
        self._flush()

    def interrupted(self) -> None:
        """Close an in-flight dispatch on the way out; a no-op when idle."""
        cur = self.doc.get("current")
        if isinstance(cur, dict) and cur.get("message") is not None:
            self.finished(cur["message"], "interrupted", None, None)


def _attempt_result(db: str | Path, mid: int) -> str | None:
    """The verdict the SUPERVISOR recorded, read back unchanged. None when the
    row is absent — the listener never recomputes an outcome, that is
    on_message.py's job."""
    try:
        con = _schema_mod().connect(db, wal=False)
    except sqlite3.Error:
        return None
    try:
        row = con.execute("SELECT state FROM dispatch_attempts"
                          " WHERE message_id = ?", (mid,)).fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None
    finally:
        con.close()


def _senders_for(db: str | Path, ids: list[int]) -> dict[int, str]:
    """Senders of the message ids a progress line mentions (ids and names
    only — never payload).

    Strictly read-only on purpose: this runs from a statusline every couple of
    seconds next to live dispatches, and may never write. The existence check
    before connect is not cosmetic — sqlite creates a file on connect, so a
    mistyped --db must not leave an empty stray database behind. Any failure
    degrades to an empty map: the line is still useful without senders.
    """
    if not ids:
        return {}
    try:
        con = _schema_mod().connect_readonly(db)
    except (OSError, ValueError, sqlite3.Error):
        return {}
    if con is None:
        return {}
    try:
        marks = ",".join("?" * len(ids))
        rows = con.execute(
            f"SELECT id, sender FROM messages WHERE id IN ({marks})",
            ids).fetchall()
        return {int(r[0]): str(r[1]) for r in rows}
    except sqlite3.Error:
        return {}
    finally:
        con.close()


# ------------------------------------------------------------------ scanner

# The dispatch set itself (legacy root tasks + task/result of a live
# conversation, bounded by message age, never by response_deadline) is
# defined once in schema.find_candidates and shared with on_message --recover.
# Exactly one candidate per scan keeps the one-worker rule; the attempt PK
# still bars anything racing.


def _expire_conversations(con: sqlite3.Connection, now: float) -> None:
    """Active conversations past expires_at move to needs_human once
    (idempotent guarded UPDATE + audit rows), so a dialogue really stops on
    its deadline even if nobody ever writes to it again."""
    ids = [r[0] for r in con.execute(
        "SELECT id FROM conversations WHERE state = 'active'"
        " AND expires_at <= ?", (now,)).fetchall()]
    if not ids:
        return
    for cid in ids:
        cur = con.execute(
            "UPDATE conversations SET state = 'needs_human', updated_at = ?"
            " WHERE id = ? AND state = 'active'",
            (now, cid),
        )
        if cur.rowcount == 1:
            con.execute(
                "INSERT INTO conversation_transitions"
                " (conversation_id, at, state, reason)"
                " VALUES (?, ?, 'needs_human', 'expired')",
                (cid, now),
            )
    con.commit()


def _scan(db: str | Path, agent: str,
          max_age: float | None = None) -> int | None:
    sm = _schema_mod()
    now = time.time()
    if max_age is None:
        max_age = sm.DEFAULT_MAX_AGE_SECONDS
    con = sm.connect(db, wal=False)
    try:
        # Guard, not the migration: run() already ensured the table, so
        # reaching here means the file is not a bridge database at all (or the
        # ensure was deferred by a lock). Reporting "nothing to do" beats an
        # OperationalError storm once a second against a foreign file.
        if not sm.has_dispatch_table(con):
            return None
        # Conversations exist only after the additive server.py migration;
        # until then (and on hand-made legacy DBs) find_candidates applies the
        # old root-task rule. Expiry bookkeeping happens here because the
        # expired dialogue may never receive another write otherwise.
        if sm.has_conversations(con):
            _expire_conversations(con, now)
        rows = sm.find_candidates(con, [agent], now, max_age, limit=1)
        return rows[0][0] if rows else None
    finally:
        con.close()


def pending(agent: str, config: str, db: str | Path) -> dict[str, Any]:
    """Read-only backlog report for one agent: never-attempted messages that
    could wake a worker, split into `dispatchable` (within
    dispatch_max_age_seconds — the listener will take them) and `stale`
    (older — a human decides; `on_message.py <id>` runs one by hand). Ids and
    ages only, never payload; nothing is written or dispatched."""
    sm = _schema_mod()
    cfg, err = sm.load_dispatch_config(config)
    out: dict[str, Any] = {
        "agent": agent, "auto": bool(cfg["auto"].get(agent, False)),
        "max_age_seconds": cfg["max_age_seconds"], "config_error": err,
        "dispatchable": [], "stale": []}
    try:
        con = sm.connect_readonly(db, timeout=2.0)
    except (OSError, ValueError, sqlite3.Error) as exc:
        out["error"] = type(exc).__name__
        return out
    if con is None:
        out["error"] = "no_database"
        return out
    try:
        if not sm.has_dispatch_table(con):
            out["error"] = "no_dispatch_table"
            return out
        now = time.time()
        for mid, _rcpt, created in sm.find_candidates(con, [agent], now, None):
            age = round(now - created, 1)
            key = "dispatchable" if age <= cfg["max_age_seconds"] else "stale"
            out[key].append({"message": mid, "age_s": age})
        return out
    except sqlite3.Error as exc:
        out["error"] = type(exc).__name__
        return out
    finally:
        con.close()


# ----------------------------------------------------------------- dispatch

def _dispatch_and_wait(agent: str, mid: int, db: str, config: str,
                       rt: Path, on_message: str,
                       progress: "_ProgressTracker | None" = None,
                       ) -> tuple[str, int | None, int | None]:
    """Run the supervisor for one message and wait for it.

    Returns (outcome, supervisor_pid, exit_code) with outcome in
    {spawn_failed, timeout, rc}: whether the supervisor never started, had to
    be killed by the watchdog, or exited on its own. What the dispatch MEANT is
    decided nowhere near here — on_message writes it to dispatch_attempts and
    the caller reads it back.
    """
    cmd = [sys.executable, str(on_message), str(mid),
           "--db", str(db), "--config", str(config), "--auto"]
    try:
        proc = subprocess.Popen(  # no shell; argv carries ids/paths only
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # own group: watchdog kills only it
        )
    except OSError as exc:
        _log(rt, f"agent={agent} spawn failed message={mid} "
                 f"kind={type(exc).__name__}")
        return ("spawn_failed", None, None)
    if progress is not None:
        progress.supervising(mid, proc.pid)
    try:
        rc = proc.wait(timeout=WORKER_SAFETY_TIMEOUT)
        _log(rt, f"agent={agent} supervisor exited message={mid} rc={rc}")
        return ("rc", proc.pid, rc)
    except subprocess.TimeoutExpired:
        _log(rt, f"agent={agent} supervisor safety timeout message={mid}, "
                 f"killing its own process group")
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
        return ("timeout", proc.pid, None)


def _sleeper(rt: Path, agent: str, total: float = POLL_SECONDS) -> bool:
    """Interruptible sleep in slices; False once the disable marker appears."""
    end = time.monotonic() + total
    while True:
        if _is_disabled(rt, agent):
            return False
        remaining = end - time.monotonic()
        if remaining <= 0:
            return True
        time.sleep(min(SLEEP_SLICE, remaining))


def run(agent: str, db: str, config: str, runtime_dir: str,
        on_message: str) -> int:
    rt = _runtime(runtime_dir)
    fd = os.open(str(_lock_path(rt, agent)),
                 os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in (errno.EACCES, errno.EAGAIN):
            # Another live listener for this agent owns the lock (duplicate
            # MCP session / headless worker): silently bow out, exit 0.
            os.close(fd)
            return 0
        os.close(fd)
        raise
    _rotate_log(rt)
    # None until the pidfile exists: the disabled-marker return below leaves
    # the loop before any dispatch, so the finally must not assume a tracker.
    progress: _ProgressTracker | None = None
    try:
        if _is_disabled(rt, agent):
            _log(rt, f"agent={agent} start skipped: disabled marker")
            return 0
        _write_private(_pid_path(rt, agent), f"{os.getpid()}\n")
        _set_state(rt, agent, "idle")
        _log(rt, f"agent={agent} listener started pid={os.getpid()}")
        # After the pidfile: progress documents are only trusted when their
        # listener_pid matches the verified pid, and this is the process that
        # will be written there. Before the loop: a fresh database has no
        # dispatch_attempts, and without it no scan can ever find work.
        progress = _ProgressTracker(rt, agent, os.getpid())
        _ensure_schema(agent, db, rt)
        fails = 0
        while True:
            if _is_disabled(rt, agent):
                _log(rt, f"agent={agent} disabled, exiting (no PID killed)")
                break
            if not _config_enabled(config, agent):
                if not _sleeper(rt, agent):
                    break
                continue
            try:
                mid = _scan(db, agent, _max_age(config))
                fails = 0
            except sqlite3.Error as exc:
                fails += 1
                delay = min(MAX_BACKOFF_SECONDS, 2.0 ** fails)
                _log(rt, f"agent={agent} scan error #{fails} "
                         f"kind={type(exc).__name__}, backoff {delay:.0f}s")
                if not _sleeper(rt, agent, delay):
                    break
                continue
            if mid is None:
                if not _sleeper(rt, agent):
                    break
                continue
            _set_state(rt, agent, f"waiting_worker:{mid}")
            _log(rt, f"agent={agent} dispatching message={mid}")
            # Written before Popen: a spawn that fails must still leave a
            # trace of what was being attempted, and the stage must never run
            # ahead of the real sequence received → supervising → finished.
            progress.received(mid)
            started = time.time()
            _outcome, _sup_pid, sup_rc = _dispatch_and_wait(
                agent, mid, db, config, rt, on_message, progress)
            # One write, whatever happened inside. The result is the
            # supervisor's own state row, so the listener cannot disagree with
            # on_message about what an outcome means.
            progress.finished(mid, _attempt_result(db, mid), sup_rc,
                              time.time() - started)
            _set_state(rt, agent, "idle")
        return 0
    finally:
        if progress is not None:
            progress.interrupted()
        _set_state(rt, agent, "idle")
        try:
            p = _pid_path(rt, agent)
            if p.read_text().strip() == str(os.getpid()):
                p.unlink()
            elif not p.exists():
                pass
        except OSError:
            pass
        _log(rt, f"agent={agent} listener stopped")
        os.close(fd)


# ------------------------------------------------------------ control calls

def _trust_progress(prog: dict[str, Any] | None,
                    pid: int | None) -> dict[str, Any] | None:
    """The document itself when it can be attributed to the listener running
    right now, else None. Trust is pid equality against the /proc-verified
    pidfile: that is what makes a listener too old to write the file, a dead
    one and a recycled pid all report "unknown" instead of a stage somebody
    else left behind. `last` is exempt from this — it is history, and showing
    it after a crash is the point."""
    if pid is None or not prog or prog.get("listener_pid") != pid:
        return None
    return prog


def status(agent: str, config: str, runtime_dir: str | Path) -> dict[str, Any]:
    """Machine-readable state for the MCP tool: ids and flags only, no payload.

    agent/enabled/running/pid/message/auto mean exactly what they meant before
    progress existed — scripts and the MCP `listener` tool keep working
    untouched. stage/progress/last are additive:

      stage "stopped"  no listener whose pid /proc confirms,
      stage "error"    the progress file exists but cannot be parsed,
      stage "starting" live listener younger than STARTING_GRACE_SECONDS,
                       still between the pidfile write and the first flush,
      stage "legacy"   live listener too old to write (no file at all) or to
                       mark its document with format_version,
      stage "unknown"  a document from a foreign pid — current is not trusted,
                       but `last` from it still is history worth showing,
      stage "idle"     it runs and has nothing in flight,
      otherwise        the stage in flight: received | supervising.

    `progress_state` is an additive diagnostic — null on the normal stages
    and otherwise unreadable | missing_file | no_format_version | stale_pid.
    `progress` is that in-flight stage (only when trusted), `last` is the
    outcome of the previous dispatch and is shown even for a dead listener —
    history is the one thing worth reading after a crash.
    """
    rt = Path(runtime_dir)
    pid = _listener_pid(rt, agent)
    message: int | None = None
    if pid is not None:
        try:
            st = _state_path(rt, agent).read_text().strip()
        except OSError:
            st = ""
        if st.startswith("waiting_worker:"):
            try:
                message = int(st.split(":", 1)[1])
            except ValueError:
                message = None
    raw, pstate = _load_progress(rt, agent)
    progress_state: str | None = None
    current: Any = None
    if pid is None:
        stage = "stopped"
    elif pstate == "unreadable":
        stage, progress_state = "error", "unreadable"
    elif pstate == "missing":
        age = _process_age_seconds(pid)
        if age is not None and age <= STARTING_GRACE_SECONDS:
            stage = "starting"
        else:
            stage, progress_state = "legacy", "missing_file"
    elif _trust_progress(raw, pid) is None:
        # Foreign or recycled pid wrote this document; its `current` describes
        # somebody else's listener, but its `last` is still the dispatch that
        # agent most recently ran, which is exactly what a reader wants.
        stage, progress_state = "unknown", "stale_pid"
    elif raw.get("format_version") != PROGRESS_FORMAT_VERSION:
        stage, progress_state = "legacy", "no_format_version"
    else:
        current = raw.get("current")
        stage = "idle" if not isinstance(current, dict) else str(
            current.get("stage") or "unknown")
    return {
        "agent": agent,
        "enabled": not _is_disabled(rt, agent),
        "running": pid is not None,
        "pid": pid,
        "message": message,
        "auto": _config_enabled(config, agent),
        "stage": stage,
        "progress_state": progress_state,
        "progress": current if isinstance(current, dict) else None,
        "last": (raw or {}).get("last"),
    }


def _fmt_age(ts: float | None) -> str:
    if not isinstance(ts, (int, float)):
        return "?"
    d = max(0.0, time.time() - float(ts))
    if d < 90:
        return f"{d:.0f}s"
    if d < 5400:
        return f"{d / 60:.0f}m"
    return f"{d / 3600:.1f}h"


def progress_line(agent: str, config: str, runtime_dir: str | Path,
                  db: str | Path | None = None) -> str:
    """One human-readable line: what the listener is doing right now, and what
    the last dispatch turned out to be. Same facts as status(), plus the
    sender of every message id mentioned (db given → `sender=` after the msg#).
    Names and ids only, never payload, prompt or tokens."""
    st = status(agent, config, runtime_dir)
    cur = st["progress"] if isinstance(st["progress"], dict) else None
    last = st["last"] if isinstance(st["last"], dict) else None
    ids = [m for m in ((cur or {}).get("message"), st["message"],
                       (last or {}).get("message")) if isinstance(m, int)]
    senders = _senders_for(db, sorted(set(ids))) if db is not None else {}

    def _seg(mid: Any) -> str:
        seg = f"msg#{mid}"
        if isinstance(mid, int) and senders.get(mid):
            seg += f" sender={senders[mid]}"
        return seg

    parts = [f"[{st['agent']}]", st["stage"]]
    if cur:
        parts.append(_seg(cur.get("message")))
        if cur.get("supervisor_pid"):
            parts.append(f"supervisor={cur['supervisor_pid']}")
        parts.append(f"for {_fmt_age(cur.get('since'))}")
    elif st["message"] is not None:
        # A listener without progress.json still tells us the id it is waiting
        # on through its state file; that stays worth showing.
        parts.append(_seg(st["message"]))
    if last is not None:
        bits = [_seg(last.get("message")),
                f"result={last.get('result') or '?'}"]
        if isinstance(last.get("duration_s"), (int, float)):
            bits.append(f"in {last['duration_s']:.1f}s")
        if last.get("supervisor_rc") is not None:
            bits.append(f"rc={last['supervisor_rc']}")
        parts.append("|")
        parts.append("last " + " ".join(bits))
    if not st["enabled"]:
        parts.append("(disabled)")
    elif not st["auto"]:
        parts.append("(auto off)")
    return " ".join(parts)


def ensure_listener(agent: str, db: str, config: str, runtime_dir: str,
                    on_message: str, env: dict[str, str] | None = None,
                    honor_env: bool = True) -> dict[str, Any]:
    """Idempotent autostart entry (MCP load / explicit enable). Never kills
    anything; dedup is the listener's flock, so a race just yields a loser
    that exits 0. Raises on spawn failure — the caller must never let that
    break the MCP session."""
    env = os.environ if env is None else env
    rt = _runtime(runtime_dir)
    if agent not in AGENTS:
        raise ValueError(f"unknown agent {agent!r}")
    if honor_env and env.get("AGENT_BRIDGE_AUTO_LISTEN", "") == "0":
        return {"status": "disabled_env"}
    if _is_disabled(rt, agent):
        return {"status": "disabled_marker"}
    if _listener_pid(rt, agent) is not None:
        return {"status": "already_running"}
    proc = subprocess.Popen(  # no shell=True
        [sys.executable, str(Path(__file__).resolve()), "run",
         "--agent", agent,
         "--db", str(db), "--config", str(config),
         "--runtime-dir", str(rt), "--on-message", str(on_message)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # the listener must outlive this MCP session
    )
    # Daemon reaper: no zombie listener child while this process lives; a
    # daemon thread never blocks shutdown, and the detached listener keeps
    # running if we exit first.
    threading.Thread(target=proc.wait, daemon=True).start()
    _log(rt, f"agent={agent} listener launched (MCP load/enable) "
             f"pid={proc.pid}")
    return {"status": "started", "pid": proc.pid}


def enable(agent: str, db: str, config: str, runtime_dir: str,
           on_message: str) -> dict[str, Any]:
    """Explicit human/tool start: removes the marker, then starts idempotently.
    An explicit enable is not blocked by AGENT_BRIDGE_AUTO_LISTEN (that env
    only forbids the *automatic* start at MCP load)."""
    rt = _runtime(runtime_dir)
    try:
        _marker_path(rt, agent).unlink(missing_ok=True)
    except OSError as exc:
        _log(rt, f"agent={agent} enable could not remove marker: "
                 f"kind={type(exc).__name__}")
    res = ensure_listener(agent, db, config, rt, on_message, honor_env=False)
    _log(rt, f"agent={agent} enable -> {res['status']}")
    out = status(agent, config, rt)
    out["action"] = res["status"]
    return out


def disable(agent: str, config: str, runtime_dir: str) -> dict[str, Any]:
    """Graceful stop: marker only. The listener takes no new task, lets the
    current worker finish and exits; nothing is ever killed by PID."""
    rt = _runtime(runtime_dir)
    _write_private(_marker_path(rt, agent),
                   f"disabled_at={time.time():.1f} by={os.getpid()}\n")
    _log(rt, f"agent={agent} disabled (marker set; graceful stop, no kill)")
    return status(agent, config, rt)


# ---------------------------------------------------------------------- CLI

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("action", choices=["run", "status", "enable", "disable",
                                      "progress", "watch", "pending"])
    p.add_argument("--agent", required=True, choices=sorted(AGENTS))
    p.add_argument("--db", default=str(DEFAULT_DB))
    p.add_argument("--config", default=str(DEFAULT_CONFIG))
    p.add_argument("--runtime-dir", default=str(DEFAULT_RUNTIME))
    p.add_argument("--on-message", default=str(DEFAULT_ON_MESSAGE))
    p.add_argument("--once", action="store_true",
                   help="with watch: print one snapshot and exit (progress"
                        " already prints exactly one, so a statusline can"
                        " call either; it is accepted here for symmetry)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    rt = str(_runtime(a.runtime_dir))
    if a.action == "run":
        return run(a.agent, a.db, a.config, rt, a.on_message)
    if a.action == "status":
        print(json.dumps(status(a.agent, a.config, rt), ensure_ascii=False),
              flush=True)
    elif a.action == "progress":
        # Exactly one snapshot, always exit 0: a statusline calls this on a
        # timer and must never see a traceback or a nonzero status. Garbage
        # config, unreadable runtime dir, foreign db — an honest unknown is
        # still a line.
        try:
            line = progress_line(a.agent, a.config, rt, a.db)
        except Exception:
            line = f"[{a.agent}] unknown"
        print(line, flush=True)
    elif a.action == "watch":
        try:
            while True:
                try:
                    line = progress_line(a.agent, a.config, rt, a.db)
                except Exception:
                    line = f"[{a.agent}] unknown"
                print(line, flush=True)
                if a.once:
                    break
                time.sleep(PROGRESS_REFRESH_SECONDS)
        except KeyboardInterrupt:
            pass
    elif a.action == "pending":
        print(json.dumps(pending(a.agent, a.config, a.db), ensure_ascii=False),
              flush=True)
    elif a.action == "enable":
        print(json.dumps(enable(a.agent, a.db, a.config, rt, a.on_message),
                         ensure_ascii=False), flush=True)
    else:
        print(json.dumps(disable(a.agent, a.config, rt), ensure_ascii=False),
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

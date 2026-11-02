#!/usr/bin/env python3
"""Tests for automatic delivery: systemd/on_message.py (per-message
supervisor) and systemd/listener.py (per-agent poller auto-started by
server.py main() at MCP load and managed by the MCP `listener` tool).

Every test runs against TEMPORARY SQLite databases and fake launchers/
supervisors/workers; no real claude or kimi is ever started. The only touch
of the live bridge.db is the idempotent CREATE-IF-NOT-EXISTS init that
`import server` performs at module load (its schema and data are not
modified; all server seams point at temp paths). The real errors.log is
detached from the logger right after import. Listener processes started by
tests use fake on_message/supervisor or fake launchers and are stopped via
the disable marker; the one SIGKILL targets only the test's own listener
pid, verified via /proc cmdline (unverified PIDs are never killed).

Run from agent_bridge/:  uv run python systemd/test_dispatcher.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent          # agent_bridge/systemd
ROOT = HERE.parent                              # agent_bridge
ON_MESSAGE = HERE / "on_message.py"
LISTENER = HERE / "listener.py"
PROMPT = HERE / "dispatcher_prompt.md"
CC_LAUNCHER = HERE / "cclocal_headless.sh"
KIMI_LAUNCHER = HERE / "kimi_headless.sh"
PY = sys.executable

SENTINEL = "SENTINEL-DISPATCH-PAYLOAD-7e21-MUST-NOT-BE-LOGGED"
TMP = Path(tempfile.mkdtemp(prefix="bridge-dispatch-tests-"))

MESSAGES_DDL = """
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
    claim_expires_at REAL,
    conversation_id INTEGER,
    kind TEXT,
    turn INTEGER
);
CREATE INDEX IF NOT EXISTS idx_messages_unread
    ON messages (recipient, acknowledged, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup
    ON messages (sender, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_conversation
    ON messages (conversation_id, id) WHERE conversation_id IS NOT NULL;
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
);
CREATE TABLE IF NOT EXISTS conversation_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL,
    at REAL NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_conv_transitions
    ON conversation_transitions (conversation_id, id);
CREATE TABLE IF NOT EXISTS dispatch_attempts (
    message_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    pid INTEGER,
    exit_code INTEGER,
    error_kind TEXT
);
"""

# The pre-conversation schema exactly as it shipped before this feature:
# used by the schema-downgrade fallback tests (listener legacy scan,
# supervisor column guards in _fetch_message/_notify_sender).
MESSAGES_DDL_LEGACY = """
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
);
CREATE INDEX IF NOT EXISTS idx_messages_unread
    ON messages (recipient, acknowledged, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup
    ON messages (sender, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS dispatch_attempts (
    message_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    pid INTEGER,
    exit_code INTEGER,
    error_kind TEXT
);
"""

FAKE_WORKER = TMP / "fake_worker.py"
FAKE_WORKER.write_text(r'''
import os, re, sqlite3, subprocess, sys, time
mode = os.environ.get("FAKE_MODE", "success")
if mode == "script":
    # Scripted multi-run worker: the file's first token is THIS run's mode
    # and is consumed; remaining tokens serve the next dispatch. Lets one
    # listener play a whole dialogue (worker: success success; reviewer:
    # review_followup review_complete).
    sp = os.environ.get("FAKE_SCRIPT", "")
    toks = []
    try:
        toks = open(sp, encoding="utf-8").read().split()
    except OSError:
        toks = []
    mode = toks[0] if toks else "silent0"
    with open(sp, "w", encoding="utf-8") as f:
        f.write(" ".join(toks[1:]))
db = os.environ.get("FAKE_DB", "")
runs = os.environ.get("FAKE_RUNS")
agent = os.environ.get("FAKE_AGENT", "claude")
prefix = {"claude": "cclocal", "kimi": "kimi"}.get(agent, "cclocal")
argv_blob = " ".join(sys.argv[1:])
try:
    data = sys.stdin.read()
except Exception:
    data = ""
via = "stdin"
if not data.strip():
    via = "file"
    m = re.search(r"(\S*request_\d+\.txt)", argv_blob)
    if m:
        try:
            data = open(m.group(1), encoding="utf-8").read()
        except OSError:
            data = ""
mm = re.search(r"message_id: (\d+)", data)
mid = int(mm.group(1)) if mm else 0
sm = re.search(r"sender: (\w+)", data)
sender = sm.group(1) if sm else "kimi"
if runs:
    with open(runs, "a", encoding="utf-8") as f:
        f.write("RAN %d via=%s agent=%s\n" % (mid, via, agent))
if os.environ.get("FAKE_ARGV_OUT"):
    with open(os.environ["FAKE_ARGV_OUT"], "a", encoding="utf-8") as f:
        f.write(argv_blob + "\n")
if mode == "dump":
    # Persist the rendered prompt; assertions on {{CONTEXT}}/{{PROTOCOL}}
    # (bounded history, role text) are then made against this file.
    dp = os.environ.get("FAKE_DUMP")
    if dp:
        with open(dp, "w", encoding="utf-8") as f:
            f.write(data)
    sys.exit(0)
if mode == "crash":
    sys.exit(7)
if mode == "silent0":
    sys.exit(0)
if mode == "sleep":
    if os.environ.get("FAKE_CHILD_PID"):
        p = subprocess.Popen(["sleep", "60"])
        with open(os.environ["FAKE_CHILD_PID"], "w", encoding="utf-8") as f:
            f.write(str(p.pid))
    time.sleep(120)
    sys.exit(0)
con = sqlite3.connect(db, timeout=5.0)
con.execute("PRAGMA busy_timeout=5000")
now = time.time()
con.execute(
    "UPDATE messages SET claimed_by='fake-worker', claim_expires_at=? WHERE id=?",
    (now + 3600, mid),
)
try:
    mrow = con.execute(
        "SELECT conversation_id, kind FROM messages WHERE id=?", (mid,)
    ).fetchone()
    cid = mrow[0] if mrow else None
    mkind = mrow[1] if mrow else None
except sqlite3.OperationalError:
    cid = mkind = None  # legacy DB: no conversation columns at all

def send(mkind2, text, dedup):
    # Minimal mirror of the server's conversation insert: next turn number,
    # the bumped turn_count and the message row in one transaction. On a
    # legacy DB (or legacy message) the conversation columns stay absent,
    # byte-identical to the pre-feature insert.
    if cid is None:
        con.execute(
            "INSERT INTO messages (sender, recipient, text, reply_to,"
            " created_at, response_deadline, dedup_key) VALUES (?,?,?,?,?,?,?)",
            (agent, sender, text, mid, now, now + 3600, dedup),
        )
        return
    turn = con.execute(
        "SELECT turn_count + 1 FROM conversations WHERE id=?", (cid,)
    ).fetchone()[0]
    con.execute(
        "UPDATE conversations SET turn_count=?, updated_at=? WHERE id=?",
        (turn, now, cid),
    )
    con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to,"
        " created_at, response_deadline, dedup_key, conversation_id,"
        " kind, turn) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (agent, sender, text, mid, now, now + 3600, dedup, cid, mkind2, turn),
    )

if cid is not None and mkind == "result" and mode.startswith("review"):
    # Reviewer protocol on a conversation result: exactly one of
    # (a) complete, (b) followup task, (c) needs_human; the reviewed message
    # is always acknowledged, no courtesy message ever.
    if mode == "review_followup":
        send("task", "следующий шаг", "%s:%d:followup" % (prefix, mid))
    else:
        target = "completed" if mode == "review_complete" else "needs_human"
        cur = con.execute(
            "UPDATE conversations SET state=?, updated_at=? WHERE id=?"
            " AND state='active'",
            (target, now, cid),
        )
        if cur.rowcount == 1:
            con.execute(
                "INSERT INTO conversation_transitions (conversation_id,"
                " at, state, reason) VALUES (?,?,?,?)",
                (cid, now, target, target),
            )
    con.execute("UPDATE messages SET acknowledged=1 WHERE id=?", (mid,))
    con.commit()
    sys.exit(0)
text = "готово" if mode == "success" else "blocked: тест-причина"
# blocked_alt_key: the same honest stop, but filed under the worker's own
# "<prefix>:<id>:blocked" key instead of the prescribed ':result' — exactly
# what the real headless worker did on live task 27.
key = "blocked" if mode == "blocked_alt_key" else "result"
send("result", text, "%s:%d:%s" % (prefix, mid, key))
if mode == "success":
    con.execute("UPDATE messages SET acknowledged=1 WHERE id=?", (mid,))
else:
    con.execute(
        "UPDATE messages SET claimed_by=NULL, claim_expires_at=NULL WHERE id=?",
        (mid,),
    )
con.commit()
sys.exit(0)
''', encoding="utf-8")

FAKE_CLAUDE_LAUNCHER = TMP / "fake_claude_launcher.sh"
FAKE_KIMI_LAUNCHER = TMP / "fake_kimi_launcher.sh"
for p in (FAKE_CLAUDE_LAUNCHER, FAKE_KIMI_LAUNCHER):
    p.write_text('#!/usr/bin/env bash\nexec "${FAKE_PY}" "${FAKE_WORKER}" "$@"\n',
                 encoding="utf-8")
    p.chmod(0o755)

FAKE_SUPERVISOR = TMP / "fake_supervisor.py"
FAKE_SUPERVISOR.write_text(r'''
import json, os, sqlite3, sys, time
argv = sys.argv[1:]
log = os.environ.get("FAKE_SUP_LOG")
if log:
    with open(log, "a", encoding="utf-8") as f:
        f.write(" ".join(argv) + "\n")
pf = os.environ.get("FAKE_SUP_SEE_PROGRESS")
if pf:
    # Snapshot of the progress document as seen by the supervisor process at
    # its own start: the listener must have written 'received' BEFORE the
    # spawn, so current here is 'received' (or 'supervising' if the parent
    # already recorded our pid). Never absent, never another message.
    try:
        with open(pf, encoding="utf-8") as f:
            seen = json.load(f).get("current")
    except Exception:
        seen = None
    with open(pf + ".seen", "w", encoding="utf-8") as f:
        json.dump(seen, f)
mid = int(argv[0]) if argv and argv[0].isdigit() else 0
db = argv[argv.index("--db") + 1] if "--db" in argv else ""
delay = float(os.environ.get("FAKE_SUP_DELAY", "0"))
if delay:
    time.sleep(delay)
if os.environ.get("FAKE_SUP_PROTOCOL") and db:
    agent = os.environ.get("FAKE_SUP_AGENT", "claude")
    prefix = {"claude": "cclocal", "kimi": "kimi"}[agent]
    con = sqlite3.connect(db, timeout=5.0)
    con.execute("PRAGMA busy_timeout=5000")
    row = con.execute("SELECT sender FROM messages WHERE id=?", (mid,)).fetchone()
    sender = row[0] if row else "kimi"
    now = time.time()
    con.execute(
        "UPDATE messages SET claimed_by='fake-sup', claim_expires_at=? WHERE id=?",
        (now + 3600, mid),
    )
    con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to, created_at,"
        " response_deadline, dedup_key) VALUES (?,?,?,?,?,?,?)",
        (agent, sender, "готово", mid, now, now + 3600,
         "%s:%d:result" % (prefix, mid)),
    )
    con.execute("UPDATE messages SET acknowledged=1 WHERE id=?", (mid,))
    con.commit()
sys.exit(0)
''', encoding="utf-8")


# ---------------------------------------------------------------- helpers

def new_area(name: str) -> Path:
    d = TMP / name
    d.mkdir(parents=True, exist_ok=True)
    return d


def new_db(d: Path, attempts: bool = True, legacy: bool = False) -> Path:
    db = d / "bridge.db"
    con = sqlite3.connect(db)
    con.executescript(MESSAGES_DDL_LEGACY if legacy else MESSAGES_DDL)
    if not attempts:
        con.execute("DROP TABLE IF EXISTS dispatch_attempts")
    con.commit()
    con.close()
    return db


def write_cfg(d: Path, claude: bool = True, kimi: bool = False) -> Path:
    cfg = d / "config.json"
    cfg.write_text(json.dumps({"auto": {"claude": claude, "kimi": kimi}}),
                   encoding="utf-8")
    return cfg


def seed(db: Path, sender: str = "kimi", recipient: str = "claude",
         text: str | None = None, reply_to: int | None = None,
         deadline: float = 3600.0, acked: int = 0,
         claim_expires: float | None = None,
         dedup: str | None = None) -> int:
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=5000")
    now = time.time()
    cur = con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to, created_at,"
        " response_deadline, acknowledged, claim_expires_at, dedup_key)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (sender, recipient,
         text if text is not None else f"задача {SENTINEL}",
         reply_to, now, now + deadline, acked, claim_expires, dedup),
    )
    con.commit()
    mid = cur.lastrowid
    con.close()
    return mid


def seed_conv(db: Path, goal: str = "цель диалога", initiator: str = "kimi",
              recipient: str = "claude", max_turns: int = 6,
              expires_in: float = 3600.0, state: str = "active") -> tuple:
    """Conversation row + root task (turn 1) exactly as start_conversation
    creates them; returns (conversation_id, root_message_id)."""
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=5000")
    now = time.time()
    cur = con.execute(
        "INSERT INTO conversations (goal, initiator, recipient, state,"
        " turn_count, max_turns, created_at, expires_at, updated_at)"
        " VALUES (?,?,?, ?,1,?,?,?,?)",
        (goal, initiator, recipient, state, max_turns, now,
         now + expires_in, now),
    )
    cid = cur.lastrowid
    con.execute(
        "INSERT INTO conversation_transitions (conversation_id, at, state,"
        " reason) VALUES (?,?,?,?)",
        (cid, now, state, "created"),
    )
    cur = con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to,"
        " created_at, response_deadline, conversation_id, kind, turn)"
        " VALUES (?,?,?,?,?,?,?,'task',1)",
        (initiator, recipient, f"задача {SENTINEL}", None, now, now + 3600,
         cid),
    )
    mid = cur.lastrowid
    con.commit()
    con.close()
    return cid, mid


def conv_msg(db: Path, cid: int, sender: str, recipient: str,
             reply_to: int, kind: str, text: str = "готово",
             dedup: str | None = None, acked: int = 0,
             deadline: float = 3600.0, bump_turns: bool = True) -> int:
    """Direct insert of a conversation message, bypassing every server
    check — for building states the real send path refuses to create."""
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=5000")
    now = time.time()
    turn = None
    if bump_turns:
        turn = con.execute(
            "SELECT turn_count + 1 FROM conversations WHERE id=?", (cid,)
        ).fetchone()[0]
        con.execute(
            "UPDATE conversations SET turn_count=?, updated_at=? WHERE id=?",
            (turn, now, cid),
        )
    cur = con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to,"
        " created_at, response_deadline, acknowledged, dedup_key,"
        " conversation_id, kind, turn) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (sender, recipient, text, reply_to, now, now + deadline, acked,
         dedup, cid, kind, turn),
    )
    mid = cur.lastrowid
    con.commit()
    con.close()
    return mid


def conv_state(db: Path, cid: int) -> str:
    return q1(db, "SELECT state FROM conversations WHERE id=?", cid)[0]


def conv_turn(db: Path, cid: int) -> int:
    return q1(db, "SELECT turn_count FROM conversations WHERE id=?", cid)[0]


def conv_cols(db: Path, mid: int):
    return q1(db, "SELECT conversation_id, kind, turn FROM messages"
                  " WHERE id=?", mid)


def conv_reasons(db: Path, cid: int) -> list:
    con = sqlite3.connect(db)
    try:
        return [r[0] for r in con.execute(
            "SELECT reason FROM conversation_transitions WHERE"
            " conversation_id=? ORDER BY id", (cid,))]
    finally:
        con.close()


def insert_attempt(db: Path, mid: int, state: str, pid: int | None,
                   started_at: float) -> None:
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=5000")
    con.execute(
        "INSERT INTO dispatch_attempts (message_id, state, started_at, pid)"
        " VALUES (?,?,?,?)",
        (mid, state, started_at, pid),
    )
    con.commit()
    con.close()


def q1(db: Path, sql: str, *args):
    con = sqlite3.connect(db)
    try:
        return con.execute(sql, args).fetchone()
    finally:
        con.close()


def attempt_row(db: Path, mid: int):
    return q1(db, "SELECT state, pid, exit_code, error_kind FROM"
                  " dispatch_attempts WHERE message_id = ?", mid)


def runs_of(d: Path) -> list[str]:
    p = d / "runs.txt"
    if not p.exists():
        return []
    return [ln for ln in p.read_text(errors="replace").splitlines()
            if ln.startswith("RAN")]


def result_row(db: Path, mid: int, prefix: str, agent: str, sender: str):
    return q1(db, "SELECT id, text, reply_to FROM messages WHERE sender = ?"
                  " AND recipient = ? AND reply_to = ? AND dedup_key = ?",
              agent, sender, mid, f"{prefix}:{mid}:result")


def notice_row(db: Path, mid: int, prefix: str = "cclocal"):
    return q1(db, "SELECT sender, text FROM messages WHERE dedup_key = ?",
              f"{prefix}:{mid}:dispatch-error")


def sup_run(d: Path, db: Path, cfg: Path, *flags: str, mode: str = "success",
            agent: str = "claude", child: Path | None = None,
            argv_out: Path | None = None, wtimeout: str = "2",
            extra_env: dict | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.update({
        "FAKE_PY": PY,
        "FAKE_WORKER": str(FAKE_WORKER),
        "FAKE_MODE": mode,
        "FAKE_DB": str(db),
        "FAKE_RUNS": str(d / "runs.txt"),
        "FAKE_AGENT": agent,
    })
    if child is not None:
        env["FAKE_CHILD_PID"] = str(child)
    if argv_out is not None:
        env["FAKE_ARGV_OUT"] = str(argv_out)
    if extra_env:
        env.update(extra_env)
    cmd = [PY, str(ON_MESSAGE), *[str(f) for f in flags],
           "--db", str(db), "--config", str(cfg),
           "--runtime-dir", str(d / "runtime"),
           "--prompt-template", str(PROMPT),
           "--claude-launcher", str(FAKE_CLAUDE_LAUNCHER),
           "--kimi-launcher", str(FAKE_KIMI_LAUNCHER),
           "--worker-timeout", wtimeout]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                          env=env)


def dead_pid() -> int:
    p = subprocess.Popen([PY, "-c", "pass"])
    p.wait()
    return p.pid


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def supervisor_log_lines(d: Path) -> list[str]:
    p = d / "sup.log"
    return p.read_text(errors="replace").splitlines() if p.exists() else []


def wait_for(cond, timeout: float = 6.0, interval: float = 0.1) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(interval)
    return cond()


# ------------------------------------------------------------------ runner

FAILURES: list[str] = []
CURRENT = ""


def check(name: str, ok: bool, detail: str = ""):
    if not ok:
        FAILURES.append(f"{CURRENT}: {name}" + (f" — {detail}" if detail else ""))


def run_test(fn):
    global CURRENT
    CURRENT = fn.__name__
    d = new_area(fn.__name__)
    try:
        fn(d)
        print(f"[PASS] {CURRENT}")
    except Exception as exc:  # noqa: BLE001 — test harness reports everything
        FAILURES.append(f"{CURRENT}: EXC {type(exc).__name__}: {exc}")
        print(f"[FAIL] {CURRENT}: {type(exc).__name__}: {exc}")


# ------------------------------------------------------- supervisor tests

def test_00_schema_idempotent(d: Path):
    db = new_db(d, attempts=False)  # supervisor must create the table itself
    cfg = write_cfg(d)
    r1 = sup_run(d, db, cfg, "--recover")
    check("first recover rc0", r1.returncode == 0, r1.stderr[-400:])
    row = q1(db, "SELECT name FROM sqlite_master WHERE type='table'"
                 " AND name='dispatch_attempts'")
    check("dispatch_attempts created", row is not None)
    r2 = sup_run(d, db, cfg, "--recover")
    check("second recover rc0", r2.returncode == 0, r2.stderr[-400:])


def test_01_auto_success_claude(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    r = sup_run(d, db, cfg, mid, "--auto")
    check("rc0", r.returncode == 0, r.stderr[-400:])
    check("worker ran via stdin", runs_of(d) == [f"RAN {mid} via=stdin agent=claude"],
          str(runs_of(d)))
    a = attempt_row(db, mid)
    check("attempt succeeded exit0", a is not None and a[0] == "succeeded"
          and a[2] == 0, str(a))
    check("original acked", q1(db, "SELECT acknowledged FROM messages"
                               " WHERE id=?", mid)[0] == 1)
    rr = result_row(db, mid, "cclocal", "claude", "kimi")
    check("result row present", rr is not None)
    check("no notice", notice_row(db, mid) is None)
    rt = d / "runtime"
    check("runtime 0700", oct(rt.stat().st_mode)[-3:] == "700")
    outs = list(rt.glob(f"worker_{mid}.out"))
    check("worker out 0600", outs and oct(outs[0].stat().st_mode)[-3:] == "600")


def test_02_auto_success_kimi(d: Path):
    db = new_db(d)
    cfg = write_cfg(d, kimi=True)
    mid = seed(db, sender="claude", recipient="kimi")
    r = sup_run(d, db, cfg, mid, "--auto", agent="kimi")
    check("rc0", r.returncode == 0, r.stderr[-400:])
    check("worker ran via request file",
          runs_of(d) == [f"RAN {mid} via=file agent=kimi"], str(runs_of(d)))
    check("attempt succeeded", attempt_row(db, mid)[0] == "succeeded")
    rr = result_row(db, mid, "kimi", "kimi", "claude")
    check("result kimi:<id>:result", rr is not None)
    req = d / "runtime" / f"request_{mid}.txt"
    check("request file exists", req.exists())
    check("request 0600", oct(req.stat().st_mode)[-3:] == "600")
    check("payload only in request file", SENTINEL in req.read_text(errors="replace"))
    argv_out = d / "argv.txt"
    # rerun argv capture on a second message to inspect the kimi argv
    mid2 = seed(db, sender="claude", recipient="kimi")
    r2 = sup_run(d, db, cfg, mid2, "--auto", agent="kimi", argv_out=argv_out)
    check("second kimi rc0", r2.returncode == 0, r2.stderr[-400:])
    blob = argv_out.read_text(errors="replace")
    check("sentinel not in worker argv", SENTINEL not in blob)
    check("kimi argv has short request instruction",
          f"request_{mid2}.txt" in blob)


def test_03_manual_mode(d: Path):
    db = new_db(d)
    cfg = write_cfg(d, claude=False, kimi=False)
    mid = seed(db, deadline=-5.0)  # expired: would be rejected in auto mode
    r = sup_run(d, db, cfg, mid)    # manual: no --auto
    check("manual bypasses gates", r.returncode == 0, r.stderr[-400:])
    check("manual ran worker", runs_of(d) == [f"RAN {mid} via=stdin agent=claude"])
    check("manual succeeded", attempt_row(db, mid)[0] == "succeeded")
    # active claim is still respected in manual mode
    mid2 = seed(db, claim_expires=time.time() + 600)
    r2 = sup_run(d, db, cfg, mid2)
    check("active claim respected", runs_of(d) == [f"RAN {mid} via=stdin agent=claude"])
    check("no attempt for claimed", attempt_row(db, mid2) is None)
    check("skip rc0", r2.returncode == 0)


def test_04_auto_skips(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    m_reply = seed(db, reply_to=1)
    m_off = seed(db)
    cfg_off = write_cfg(new_area("t04cfg"), claude=False)
    m_dead = seed(db, deadline=-10.0)
    m_claim = seed(db, claim_expires=time.time() + 600)
    m_ack = seed(db, acked=1)
    m_att = seed(db)
    insert_attempt(db, m_att, "failed", None, time.time())
    r = sup_run(d, db, cfg, m_reply, "--auto")
    check("reply skipped rc0", r.returncode == 0)
    sup_run(d, db, cfg_off, m_off, "--auto")
    sup_run(d, db, cfg, m_dead, "--auto")
    sup_run(d, db, cfg, m_claim, "--auto")
    sup_run(d, db, cfg, m_ack, "--auto")
    r_att = sup_run(d, db, cfg, m_att, "--auto")
    check("existing attempt skip rc0", r_att.returncode == 0)
    check("no worker ran at all", runs_of(d) == [])
    for m in (m_reply, m_off, m_dead, m_claim, m_ack):
        check(f"no attempt row for {m}", attempt_row(db, m) is None)
    check("preexisting attempt untouched", attempt_row(db, m_att)[0] == "failed")


def test_05_concurrent_single_execution(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    env_base = dict(os.environ)
    env_base.update({
        "FAKE_PY": PY, "FAKE_WORKER": str(FAKE_WORKER), "FAKE_MODE": "success",
        "FAKE_DB": str(db), "FAKE_RUNS": str(d / "runs.txt"), "FAKE_AGENT": "claude",
    })
    cmd = [PY, str(ON_MESSAGE), str(mid), "--auto",
           "--db", str(db), "--config", str(cfg),
           "--runtime-dir", str(d / "runtime"),
           "--prompt-template", str(PROMPT),
           "--claude-launcher", str(FAKE_CLAUDE_LAUNCHER),
           "--kimi-launcher", str(FAKE_KIMI_LAUNCHER),
           "--worker-timeout", "10"]
    procs = [subprocess.Popen(cmd, env=env_base, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL) for _ in range(2)]
    rcs = [p.wait(timeout=30) for p in procs]
    check("both supervisors exit 0", rcs == [0, 0], str(rcs))
    check("exactly one worker run", len(runs_of(d)) == 1, str(runs_of(d)))
    n = q1(db, "SELECT COUNT(*) FROM dispatch_attempts WHERE message_id=?", mid)[0]
    check("exactly one attempt row", n == 1)


def test_06_crash(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    r = sup_run(d, db, cfg, mid, "--auto", mode="crash")
    check("rc3", r.returncode == 3, str(r.returncode))
    a = attempt_row(db, mid)
    check("failed exit7", a is not None and a[0] == "failed" and a[2] == 7, str(a))
    n = notice_row(db, mid)
    check("notice sent", n is not None and n[0] == "claude")
    check("notice without payload", n is not None and SENTINEL not in n[1])
    check("original not acked", q1(db, "SELECT acknowledged FROM messages"
                                   " WHERE id=?", mid)[0] == 0)
    # a second supervisor for the same message must not rerun or re-notify
    sup_run(d, db, cfg, mid, "--auto", mode="crash")
    check("no rerun", runs_of(d) == [f"RAN {mid} via=stdin agent=claude"])
    cnt = q1(db, "SELECT COUNT(*) FROM messages WHERE dedup_key=?",
             f"cclocal:{mid}:dispatch-error")[0]
    check("single notice", cnt == 1)


def test_07_silent0(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    r = sup_run(d, db, cfg, mid, "--auto", mode="silent0")
    check("rc3", r.returncode == 3)
    check("uncertain", attempt_row(db, mid)[0] == "uncertain")
    check("notice sent", notice_row(db, mid) is not None)
    check("no result row", result_row(db, mid, "cclocal", "claude", "kimi") is None)


def test_08_blocked(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    r = sup_run(d, db, cfg, mid, "--auto", mode="blocked")
    check("rc0", r.returncode == 0, str(r.returncode))
    check("blocked state", attempt_row(db, mid)[0] == "blocked")
    check("no notice for blocked", notice_row(db, mid) is None)
    check("original stays unacked", q1(db, "SELECT acknowledged FROM messages"
                                        " WHERE id=?", mid)[0] == 0)
    rr = result_row(db, mid, "cclocal", "claude", "kimi")
    check("blocked result exists", rr is not None and "blocked" in rr[1])


def test_09_timeout_kills_group(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    child = d / "child.pid"
    r = sup_run(d, db, cfg, mid, "--auto", mode="sleep", child=child,
                wtimeout="1")
    check("rc3", r.returncode == 3, str(r.returncode))
    a = attempt_row(db, mid)
    check("timeout state", a is not None and a[0] == "timeout", str(a))
    check("notice sent", notice_row(db, mid) is not None)
    cpid = int(child.read_text())
    gone = wait_for(lambda: not pid_alive(cpid), timeout=5.0)
    check("worker child killed, no orphans", gone)


def test_11_reconcile_and_recover(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    now = time.time()
    m_new = seed(db)                                    # recoverable
    m_unc = seed(db)                                    # has uncertain attempt
    m_dead = seed(db, deadline=-30.0)
    m_reply = seed(db, reply_to=1)
    m_kimi = seed(db, sender="claude", recipient="kimi")
    insert_attempt(db, m_unc, "uncertain", None, now)
    s1 = seed(db)
    insert_attempt(db, s1, "spawned", dead_pid(), now - 3600)
    s2 = seed(db)
    insert_attempt(db, s2, "spawned", None, now - 120)
    s3 = seed(db)
    insert_attempt(db, s3, "spawned", None, now - 3)
    s4 = seed(db)
    s4pid = dead_pid()
    insert_attempt(db, s4, "spawned", s4pid, now - 3600)
    # evidence of completed protocol for s4: result + ack
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to, created_at,"
        " response_deadline, dedup_key) VALUES (?,?,?,?,?,?,?)",
        ("claude", "kimi", "готово", s4, now, now + 60, f"cclocal:{s4}:result"),
    )
    con.execute("UPDATE messages SET acknowledged=1 WHERE id=?", (s4,))
    con.commit()
    con.close()

    r = sup_run(d, db, cfg, "--recover")
    check("recover rc0", r.returncode == 0, r.stderr[-500:])
    check("only the attemptless root ran", runs_of(d) == [f"RAN {m_new} via=stdin agent=claude"],
          str(runs_of(d)))
    check("new succeeded", attempt_row(db, m_new)[0] == "succeeded")
    check("uncertain untouched", attempt_row(db, m_unc)[0] == "uncertain")
    check("expired deadline skipped", attempt_row(db, m_dead) is None)
    check("reply skipped", attempt_row(db, m_reply) is None)
    check("kimi config-off skipped", attempt_row(db, m_kimi) is None)
    check("stale dead pid -> interrupted", attempt_row(db, s1)[0] == "interrupted")
    check("grace-expired null pid -> interrupted", attempt_row(db, s2)[0] == "interrupted")
    check("fresh null pid left alone", attempt_row(db, s3)[0] == "spawned")
    check("stale with evidence -> succeeded", attempt_row(db, s4)[0] == "succeeded")


def test_16_rotation(d: Path):
    db = new_db(d)
    cfg = write_cfg(d, kimi=True)
    for i in range(23):
        mid = seed(db)
        r = sup_run(d, db, cfg, mid, "--auto")
        if r.returncode != 0:
            check(f"claude run {i} rc0", False, r.stderr[-300:])
    outs = list((d / "runtime").glob("worker_*.out"))
    check("worker out rotation keep-20", len(outs) == 20, str(len(outs)))
    for i in range(23):
        mid = seed(db, sender="claude", recipient="kimi")
        r = sup_run(d, db, cfg, mid, "--auto", agent="kimi")
        if r.returncode != 0:
            check(f"kimi run {i} rc0", False, r.stderr[-300:])
    outs = list((d / "runtime").glob("worker_*.out"))
    check("worker out still keep-20", len(outs) == 20, str(len(outs)))
    reqs = list((d / "runtime").glob("request_*.txt"))
    check("request rotation keep-20", len(reqs) == 20, str(len(reqs)))


def test_17_missing_db(d: Path):
    cfg = write_cfg(d)
    r = sup_run(d, d / "nope.db", cfg, 1, "--auto")
    check("rc2 on missing db", r.returncode == 2, str(r.returncode))


def test_15_payload_safety(d: Path):
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    argv_out = d / "argv.txt"
    r = sup_run(d, db, cfg, mid, "--auto", mode="crash", argv_out=argv_out)
    check("crash rc3", r.returncode == 3)
    runtime = d / "runtime"
    blob = (runtime / "dispatcher.log").read_text(errors="replace")
    check("no payload in dispatcher.log", SENTINEL not in blob)
    check("no payload in worker output",
          SENTINEL not in (runtime / f"worker_{mid}.out").read_text(errors="replace"))
    check("no payload in worker argv", SENTINEL not in argv_out.read_text(errors="replace"))
    n = notice_row(db, mid)
    check("no payload in notice", n is not None and SENTINEL not in n[1])


def test_18_launcher_stubs(d: Path):
    # cclocal launcher: whitelisted env rebuild, token only via child env,
    # nothing printed.
    env_file = d / "aliases"
    cfgdir = d / "cfgdir"
    stub_out = d / "stub_env.txt"
    stub_bin = d / "claude_stub.sh"
    env_file.write_text(
        'export LOCAL_CLAUDE_BASE_URL="http://stub.local:8080"\n'
        f'export LOCAL_CLAUDE_CONFIG_DIR="{cfgdir}"\n'
        'export LOCAL_CLAUDE_AUTH_TOKEN="SECRET-TOKEN-913"\n'
        'export LOCAL_CLAUDE_MODEL="stub-model"\n'
        'echo "MUST NOT EVAL"\n',
        encoding="utf-8",
    )
    stub_bin.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$ANTHROPIC_BASE_URL" "$ANTHROPIC_AUTH_TOKEN" '
        '"$ANTHROPIC_API_KEY" "$ANTHROPIC_MODEL" "$CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC" '
        '> "$STUB_OUT"\n'
        'printf \'%s\\n\' "$@"\n',
        encoding="utf-8",
    )
    stub_bin.chmod(0o755)
    base_env = {k: v for k, v in os.environ.items()
                if not k.startswith(("LOCAL_CLAUDE_", "ANTHROPIC_", "CLAUDE_"))}
    r = subprocess.run(
        ["bash", str(CC_LAUNCHER), "--print", "--output-format", "json"],
        capture_output=True, text=True, timeout=15,
        env={**base_env, "CCLOCAL_ENV_FILE": str(env_file),
             "CCLOCAL_CLAUDE_BIN": str(stub_bin), "STUB_OUT": str(stub_out)},
    )
    check("cclocal launcher rc0", r.returncode == 0, r.stderr[-300:])
    check("launcher stdout only stub args",
          r.stdout.splitlines() == ["--print", "--output-format", "json"], r.stdout)
    check("whitelist skipped foreign rc line", "MUST NOT EVAL" not in r.stdout + r.stderr)
    vals = stub_out.read_text().splitlines()
    check("base url rebuilt", vals[0] == "http://stub.local:8080", str(vals))
    check("auth token passed via env", vals[1] == "SECRET-TOKEN-913")
    check("api key empty", vals[2] == "")
    check("model rebuilt", vals[3] == "stub-model")
    check("nonessential traffic off", vals[4] == "1")

    # kimi launcher: argv passthrough + workspace trust, no fake permission flag
    kimi_out = d / "kimi_stub.txt"
    kimi_stub = d / "kimi_stub.sh"
    kimi_stub.write_text(
        "#!/usr/bin/env bash\n"
        '{ printf \'%s\\n\' "$@"; echo "TRUST=$KIMI_CODE_TRUST_WORKSPACE"; } '
        '> "$KIMI_STUB_OUT"\n',
        encoding="utf-8",
    )
    kimi_stub.chmod(0o755)
    r2 = subprocess.run(
        ["bash", str(KIMI_LAUNCHER), "--prompt", "короткая строка",
         "--output-format", "stream-json"],
        capture_output=True, text=True, timeout=15,
        env={**os.environ, "KIMI_BIN": str(kimi_stub),
             "KIMI_STUB_OUT": str(kimi_out),
             "KIMI_CODE_TRUST_WORKSPACE": ""},
    )
    check("kimi launcher rc0", r2.returncode == 0, r2.stderr[-300:])
    lines = kimi_out.read_text().splitlines()
    check("kimi argv passthrough",
          lines[:4] == ["--prompt", "короткая строка", "--output-format", "stream-json"],
          str(lines))
    check("kimi workspace trusted", lines[4] == "TRUST=1", str(lines))


# --------------------------------------------------------- listener tests

_server = None


def import_server(tmp_errors: Path):
    global _server
    if _server is None:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import server  # noqa: F401 — module-level init touches live DB idempotently
        _server = server
    lg = logging.getLogger("agent_bridge")
    for h in list(lg.handlers):
        lg.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    lg.addHandler(logging.FileHandler(tmp_errors, encoding="utf-8"))
    return _server


def set_server_seams(server, d: Path, db: Path, cfg: Path,
                     on_message: Path = FAKE_SUPERVISOR) -> Path:
    """Point every server test seam at this temp area; the real listener.py
    code runs, only the paths around it are swapped."""
    rt = d / "runtime"
    server.DB_PATH = db
    server.LISTENER_PY = LISTENER
    server.RUNTIME_DIR = rt
    server.DISPATCH_CONFIG_PATH = cfg
    server.ON_MESSAGE = on_message
    return rt


def listener_env(**fake: str) -> dict[str, str]:
    env = dict(os.environ)
    env.pop("AGENT_BRIDGE_AUTO_LISTEN", None)
    env.update({k: str(v) for k, v in fake.items()})
    return env


def listener_cmd(action: str, agent: str, rt: Path, *, db: Path | None = None,
                 cfg: Path | None = None,
                 on_message: Path | None = None) -> list[str]:
    cmd = [PY, str(LISTENER), action, "--agent", agent,
           "--runtime-dir", str(rt)]
    if db is not None:
        cmd += ["--db", str(db)]
    if cfg is not None:
        cmd += ["--config", str(cfg)]
    if on_message is not None:
        cmd += ["--on-message", str(on_message)]
    return cmd


def listener_cli(action: str, agent: str, rt: Path, *, db: Path | None = None,
                 cfg: Path | None = None, on_message: Path | None = None,
                 env: dict | None = None, timeout: int = 20):
    return subprocess.run(
        listener_cmd(action, agent, rt, db=db, cfg=cfg, on_message=on_message),
        capture_output=True, text=True, timeout=timeout,
        env=env if env is not None else listener_env())


def listener_status(agent: str, rt: Path, cfg: Path) -> dict:
    return json.loads(listener_cli("status", agent, rt, cfg=cfg).stdout)


def spawn_listener(agent: str, rt: Path, *, db: Path, cfg: Path,
                   on_message: Path, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        listener_cmd("run", agent, rt, db=db, cfg=cfg, on_message=on_message),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, env=env)


def stop_listener(agent: str, rt: Path, cfg: Path,
                  proc: subprocess.Popen | None = None) -> bool:
    """Graceful stop via the disable marker (never a signal to a listener);
    the Popen handle is only reaped here."""
    listener_cli("disable", agent, rt, cfg=cfg)
    ok = wait_for(lambda: listener_status(agent, rt, cfg)["running"] is False,
                  timeout=8.0)
    if proc is not None:
        if proc.poll() is None:
            proc.kill()  # only our own child handle, after the marker failed
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    return ok


def fake_sup_env(d: Path, sup_agent: str = "claude") -> dict:
    return listener_env(FAKE_SUP_LOG=str(d / "sup.log"),
                        FAKE_SUP_PROTOCOL="1",
                        FAKE_SUP_AGENT=sup_agent)


def fake_worker_env(d: Path, db: Path, mode: str = "success",
                    agent: str = "claude") -> dict:
    return listener_env(FAKE_PY=PY, FAKE_WORKER=str(FAKE_WORKER),
                        FAKE_MODE=mode, FAKE_DB=str(db),
                        FAKE_RUNS=str(d / "runs.txt"), FAKE_AGENT=agent,
                        FAKE_ARGV_OUT=str(d / "argv.txt"))


def real_om_wrapper(d: Path, rt: Path, name: str = "om_wrapper.py",
                    wtimeout: str = "5") -> Path:
    """The REAL on_message.py plus this test's fake launchers: the listener
    calls on_message as `<id> --db --config --auto`, so launcher/runtime/prompt
    injection lives only here, never in listener.py. execv keeps it one
    process."""
    w = d / name
    w.write_text(
        "import os, sys\n"
        "os.execv(sys.executable, [sys.executable, %r, *sys.argv[1:],\n"
        "  '--runtime-dir', %r, '--prompt-template', %r,\n"
        "  '--claude-launcher', %r, '--kimi-launcher', %r,\n"
        "  '--worker-timeout', %r])\n"
        % (str(ON_MESSAGE), str(rt), str(PROMPT),
           str(FAKE_CLAUDE_LAUNCHER), str(FAKE_KIMI_LAUNCHER), wtimeout),
        encoding="utf-8")
    return w


def test_12_listener_autostart_survives_mcp_exit(d: Path):
    # A short-lived MCP session (main() returns when mcp.run exits) starts the
    # listener for its own AGENT_ID and that process outlives the session.
    db = new_db(d)
    cfg = write_cfg(d)                       # claude on, kimi off
    rt = d / "runtime"
    harness = TMP / "harness_12.py"
    harness.write_text(f'''
import os, sys
sys.path.insert(0, {str(ROOT)!r})
import server
server.DB_PATH = {str(db)!r}
server.LISTENER_PY = {str(LISTENER)!r}
server.RUNTIME_DIR = {str(rt)!r}
server.DISPATCH_CONFIG_PATH = {str(cfg)!r}
server.ON_MESSAGE = {str(FAKE_SUPERVISOR)!r}
server._init_db()
server.mcp.run = lambda **kw: None           # the MCP session opens and ends
server.main()
print("session-exited", flush=True)
os._exit(0)                                  # session process dies for real
''', encoding="utf-8")
    for agent in ("kimi", "claude"):
        r = subprocess.run([PY, str(harness)], capture_output=True, text=True,
                           timeout=30, env=listener_env(AGENT_ID=agent,
                                                        FAKE_SUP_LOG=str(d / "sup.log"),
                                                        FAKE_SUP_PROTOCOL="1",
                                                        FAKE_SUP_AGENT="claude"))
        check(f"{agent}: mcp session exited",
              r.stdout.strip() == "session-exited", r.stdout + r.stderr[-400:])
        st = listener_status(agent, rt, cfg)  # its parent process is already dead
        check(f"{agent}: listener alive after MCP exit",
              bool(st["running"] and st["enabled"] and st["pid"]), str(st))
    # the claude listener dispatches the root task through the on_message CLI
    mid = seed(db, sender="kimi", recipient="claude")
    done = wait_for(lambda: len(supervisor_log_lines(d)) == 1, timeout=10.0)
    lines = supervisor_log_lines(d)
    check("claude listener ran on_message once for root task", done, str(lines))
    check("on_message got id/--db/--config/--auto",
          lines and lines[0].startswith(f"{mid} --db {db} --config {cfg} --auto"),
          str(lines))
    check("worker completed claim→result→ack", wait_for(
        lambda: (q1(db, "SELECT acknowledged FROM messages WHERE id=?", mid)
                 or (0,))[0] == 1
        and result_row(db, mid, "cclocal", "claude", "kimi") is not None,
        timeout=8.0))
    # auto.kimi=false stays in force: kimi's listener idles, never dispatches
    seed(db, sender="claude", recipient="kimi")
    time.sleep(3.0)
    check("kimi listener gated by config (auto.kimi=false)",
          len(supervisor_log_lines(d)) == 1, str(supervisor_log_lines(d)))
    check("kimi listener still running while gated",
          listener_status("kimi", rt, cfg)["running"])
    ok1 = stop_listener("kimi", rt, cfg)
    ok2 = stop_listener("claude", rt, cfg)
    check("both listeners stopped gracefully via marker", ok1 and ok2)


def test_13_listener_singleton_lock(d: Path):
    db = new_db(d)
    cfg = write_cfg(d, claude=True, kimi=False)
    rt = d / "runtime"
    env = fake_sup_env(d, "claude")
    p1 = spawn_listener("claude", rt, db=db, cfg=cfg,
                        on_message=FAKE_SUPERVISOR, env=env)
    try:
        check("first listener up",
              wait_for(lambda: listener_status("claude", rt, cfg)["running"],
                       timeout=8.0))
        pid1 = listener_status("claude", rt, cfg)["pid"]
        # duplicate start (second MCP session / headless worker): exits 0 fast
        t0 = time.time()
        p2 = subprocess.run(
            listener_cmd("run", "claude", rt, db=db, cfg=cfg,
                         on_message=FAKE_SUPERVISOR),
            capture_output=True, text=True, timeout=10, env=env)
        check("duplicate listener exits 0 immediately",
              p2.returncode == 0 and (time.time() - t0) < 5.0
              and p2.stdout == "", f"rc={p2.returncode} out={p2.stdout!r}")
        st = listener_status("claude", rt, cfg)
        check("pidfile still the first listener",
              st["pid"] == pid1 and st["running"], str(st))
        # in-process ensure (the autostart path) is idempotent too
        server = import_server(d / "server_errors.log")
        set_server_seams(server, d, db, cfg)
        os.environ["AGENT_ID"] = "claude"
        mod = server._get_listener()
        r1 = mod.ensure_listener("claude", str(db), str(cfg), str(rt),
                                 str(FAKE_SUPERVISOR))
        r2 = mod.ensure_listener("claude", str(db), str(cfg), str(rt),
                                 str(FAKE_SUPERVISOR))
        check("ensure_listener idempotent while running",
              r1["status"] == "already_running"
              and r2["status"] == "already_running", f"{r1} {r2}")
        # despite all those starters, one root task gets exactly one worker
        mid = seed(db)
        check("single dispatch", wait_for(
            lambda: len(supervisor_log_lines(d)) == 1, timeout=10.0))
        time.sleep(2.0)
        check("no duplicate dispatch", len(supervisor_log_lines(d)) == 1,
              str(supervisor_log_lines(d)))
    finally:
        stop_listener("claude", rt, cfg, proc=p1)
    check("listener process reaped", p1.poll() is not None)
    # zombie hygiene: every child we launched and waited on left nothing behind
    zombies = []
    for task in Path("/proc/self/task").iterdir():
        try:
            kids = (task / "children").read_text().split()
        except OSError:
            continue
        for kid in kids:
            try:
                stat = Path(f"/proc/{kid}/stat").read_text()
            except OSError:
                continue
            if stat[stat.rindex(")") + 2] == "Z":
                zombies.append(kid)
    check("no zombie children after listener exit", not zombies, str(zombies))


def test_14_listener_disable_enable_and_autostart_gates(d: Path):
    # Exercises the real MCP tool surface (server.listener) and the real
    # autostart path (server._ensure_listener_at_load): marker and env gates.
    db = new_db(d)
    cfg = write_cfg(d)
    server = import_server(d / "server_errors.log")
    rt = set_server_seams(server, d, db, cfg)
    os.environ["AGENT_ID"] = "claude"
    try:
        st = server.listener(action="status")
        check("fresh status: enabled idle",
              st["enabled"] is True and st["running"] is False
              and st["pid"] is None and st["auto"] is True, str(st))
        pr = server.listener(action="progress")
        check("progress on fresh runtime: one stopped line",
              isinstance(pr.get("line"), str)
              and pr["line"].startswith("[claude]")
              and "stopped" in pr["line"], str(pr))
        r1 = server.listener(action="enable")
        check("enable starts", r1["action"] == "started", str(r1))
        check("enable reports running",
              wait_for(lambda: server.listener(action="status")["running"],
                       timeout=8.0))
        pid1 = server.listener(action="status")["pid"]
        r2 = server.listener(action="enable")
        check("enable idempotent",
              r2["action"] == "already_running" and r2["pid"] == pid1, str(r2))

        r3 = server.listener(action="disable")
        check("disable marks and reports", r3["enabled"] is False, str(r3))
        check("listener exits gracefully after disable",
              wait_for(lambda: server.listener(action="status")["running"]
                       is False, timeout=8.0))
        # the marker survives further MCP loads (autostart attempts)
        server._ensure_listener_at_load()
        server._ensure_listener_at_load()
        st = server.listener(action="status")
        check("marker blocks autostart",
              st["enabled"] is False and st["running"] is False, str(st))

        # AGENT_BRIDGE_AUTO_LISTEN=0 blocks only the automatic start
        os.environ["AGENT_BRIDGE_AUTO_LISTEN"] = "0"
        (rt / "claude.disabled").unlink(missing_ok=True)
        server._ensure_listener_at_load()
        st = server.listener(action="status")
        check("env blocks autostart, enabled still true",
              st["enabled"] is True and st["running"] is False, str(st))
        # explicit enable overrides the env gate (human/tool decision wins)
        r4 = server.listener(action="enable")
        check("explicit enable ignores env", r4["action"] == "started",
              str(r4))
        check("explicit enable reaches running",
              wait_for(lambda: server.listener(action="status")["running"],
                       timeout=8.0))
        pr2 = server.listener(action="progress")
        check("progress on running listener: idle line",
              "idle" in pr2.get("line", ""), str(pr2))

        try:
            server.listener(action="bogus")
            check("unknown action raises ValueError", False)
        except ValueError:
            check("unknown action raises ValueError", True)
    finally:
        os.environ.pop("AGENT_BRIDGE_AUTO_LISTEN", None)
        stop_listener("claude", rt, cfg)


def test_19_listener_e2e_claude_no_retry(d: Path):
    # Full chain: listener → REAL on_message.py → fake launcher → fake
    # worker. One root task, exactly one worker, succeeded attempt, no retry.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    om = real_om_wrapper(d, rt)
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db))
    try:
        mid = seed(db)                       # kimi→claude root task
        ok = wait_for(lambda: (attempt_row(db, mid) or ("",))[0]
                      == "succeeded", timeout=15.0)
        check("attempt succeeded end-to-end", ok,
              str(attempt_row(db, mid)))
        check("original acknowledged",
              (q1(db, "SELECT acknowledged FROM messages WHERE id=?", mid)
               or (0,))[0] == 1)
        check("result message stored",
              result_row(db, mid, "cclocal", "claude", "kimi") is not None)
        check("worker ran once via stdin",
              runs_of(d) == [f"RAN {mid} via=stdin agent=claude"],
              str(runs_of(d)))
        time.sleep(2.5)
        check("no second run", len(runs_of(d)) == 1, str(runs_of(d)))
        check("one attempt row",
              q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0] == 1)
        log = (rt / "dispatcher.log").read_text(errors="replace") \
            if (rt / "dispatcher.log").exists() else ""
        check("listener log mentions dispatch", "dispatching message" in log,
              log[-200:])
        check("payload never in listener/dispatcher log",
              SENTINEL not in log)
        argv = (d / "argv.txt").read_text(errors="replace") \
            if (d / "argv.txt").exists() else ""
        check("payload never in argv", SENTINEL not in argv, argv[-200:])
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_20_listener_ignores_non_candidates(d: Path):
    # reply_to / expired deadline / live claim / acknowledged must never wake
    # a worker; only a fresh root task does.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    p = spawn_listener("claude", rt, db=db, cfg=cfg,
                       on_message=FAKE_SUPERVISOR, env=fake_sup_env(d))
    try:
        seed(db, reply_to=999)                       # result-shaped
        seed(db, deadline=-10.0)                     # expired deadline
        seed(db, claim_expires=time.time() + 3600)   # live claim
        seed(db, acked=1)                            # acknowledged
        time.sleep(3.0)
        check("non-candidates launch nothing",
              supervisor_log_lines(d) == [], str(supervisor_log_lines(d)))
        mid = seed(db)                               # real root task
        check("root task dispatched", wait_for(
            lambda: len(supervisor_log_lines(d)) == 1, timeout=10.0))
        time.sleep(2.0)
        check("still exactly one dispatch",
              len(supervisor_log_lines(d)) == 1,
              str(supervisor_log_lines(d)))
        check("candidate was the root",
              supervisor_log_lines(d)[0].startswith(f"{mid} --db"),
              supervisor_log_lines(d)[0])
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_21_two_listeners_one_execution(d: Path):
    # Deliberate misconfiguration (same agent, two runtime dirs → two live
    # listeners, flock cannot see across dirs): the dispatch_attempts PK is
    # still the barrier — one worker, one attempt, one result.
    db = new_db(d)
    cfg = write_cfg(d)
    rt1, rt2 = d / "rt1", d / "rt2"
    env = fake_worker_env(d, db)
    p1 = spawn_listener("claude", rt1, db=db, cfg=cfg,
                        on_message=real_om_wrapper(d, rt1, "om1.py"), env=env)
    p2 = spawn_listener("claude", rt2, db=db, cfg=cfg,
                        on_message=real_om_wrapper(d, rt2, "om2.py"), env=env)
    try:
        mid = seed(db)
        check("attempt row appears", wait_for(
            lambda: attempt_row(db, mid) is not None, timeout=12.0))
        time.sleep(3.5)
        rans = [ln for ln in runs_of(d) if f"RAN {mid}" in ln]
        check("exactly one worker under two listeners", len(rans) == 1,
              str(rans))
        check("exactly one attempt row",
              q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0] == 1)
        check("exactly one result row",
              result_row(db, mid, "cclocal", "claude", "kimi") is not None)
    finally:
        stop_listener("claude", rt1, cfg, proc=p1)
        stop_listener("claude", rt2, cfg, proc=p2)


def test_22_listener_no_retry_after_crash_or_restart(d: Path):
    # A crashed worker: failed attempt + notice, no re-dispatch; SIGKILLing
    # the listener and starting a fresh one still must not re-run the task.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    om = real_om_wrapper(d, rt)
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db, mode="crash"))
    p2 = None
    try:
        mid = seed(db)
        ok = wait_for(lambda: attempt_row(db, mid) is not None
                      and attempt_row(db, mid)[0] in ("failed", "uncertain"),
                      timeout=15.0)
        check("crash recorded as failed/uncertain", ok,
              str(attempt_row(db, mid)))
        check("failure notice to sender", notice_row(db, mid) is not None)
        time.sleep(3.0)
        check("no automatic retry by the same listener",
              len(runs_of(d)) == 1, str(runs_of(d)))
        check("listener itself stays alive",
              listener_status("claude", rt, cfg)["running"] is True)
        # kill only our own listener, pid verified via /proc by status
        pid = listener_status("claude", rt, cfg)["pid"]
        check("verified listener pid", pid is not None and pid_alive(pid))
        os.kill(pid, signal.SIGKILL)
        check("killed listener disappears", wait_for(
            lambda: listener_status("claude", rt, cfg)["running"] is False,
            timeout=8.0))
        p.wait(timeout=5)
        p2 = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                            env=fake_worker_env(d, db, mode="crash"))
        time.sleep(3.0)
        check("fresh listener does not retry the failed task",
              len(runs_of(d)) == 1, str(runs_of(d)))
        check("still one attempt row",
              q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0] == 1)
    finally:
        stop_listener("claude", rt, cfg,
                      proc=p2 if p2 is not None else p)


def test_10_no_loop_results_ignite_nothing(d: Path):
    # Result-shaped and notice-shaped messages (reply_to set) must never wake
    # a worker — verified on both bridge sides at supervisor level.
    db = new_db(d)
    cfg = write_cfg(d, claude=True, kimi=True)
    task_c = seed(db)                    # kimi→claude root, already processed
    seed(db, reply_to=task_c, sender="claude", recipient="kimi",
         text=f"готово (результат) {SENTINEL}",
         dedup=f"cclocal:{task_c}:result")
    notice_to_kimi = seed(db, sender="claude", recipient="kimi",
                          text="уведомление о сбое попытки",
                          reply_to=task_c, dedup="cclocal:999:dispatch-error")
    task_k = seed(db, sender="claude", recipient="kimi")
    r1 = sup_run(d, db, cfg, notice_to_kimi, "--auto", agent="kimi")
    check("notice to kimi ran nothing", runs_of(d) == [])
    check("notice rc0", r1.returncode == 0)
    # recipient=kimi must be executed by the kimi side: worker gets the
    # request file (via=file) and the result carries the kimi prefix.
    r2 = sup_run(d, db, cfg, task_k, "--auto", agent="kimi")
    check("kimi task launcher selection rc0", r2.returncode == 0, r2.stderr[-400:])
    check("kimi result dedup", result_row(db, task_k, "kimi", "kimi", "claude") is not None)
    # the root task_k got a worker; count exactly the root runs so far
    rans = runs_of(d)
    root_runs = [ln for ln in rans if f"RAN {task_k}" in ln]
    check("root kimi task executed once", len(root_runs) == 1, str(rans))
    # and its result/notice rows never create attempts
    rows = q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0]
    check("attempts only for root", rows == 1, str(rows))


# ------------------------------------------------------ conversation tests

def test_30_conversation_send_rules(d: Path):
    # Server-level rules of a bounded dialogue: turn accounting, pairing,
    # one-message-in-flight (whole answer chain exempt), direction, dedup
    # idempotence, and a status view that never leaks the goal text.
    server = import_server(d / "server_errors.log")
    db = d / "bridge.db"
    cfg = write_cfg(d)
    set_server_seams(server, d, db, cfg)
    server._init_db()
    goal = "ЦЕЛЬ-9f31-НЕ-ДЛЯ-СТАТУСА"
    os.environ["AGENT_ID"] = "kimi"
    try:
        r = server.start_conversation("claude", goal, max_turns=6)
        cid, root = r["conversation_id"], r["message_id"]
        check("root is turn 1 active task", r["kind"] == "task"
              and r["turn"] == 1 and r["state"] == "active", str(r))
        check("root row linked", conv_cols(db, root) == (cid, "task", 1),
              str(conv_cols(db, root)))
        a = server.start_conversation("claude", "другая цель",
                                      dedup_key="t30root")
        b = server.start_conversation("claude", "другая цель",
                                      dedup_key="t30root")
        check("dedup root idempotent, no parallel branch",
              b.get("duplicate") is True and b["message_id"] == a["message_id"],
              str(b))
        os.environ["AGENT_ID"] = "claude"
        r1 = server.send_message("kimi", "готово", reply_to=root,
                                 kind="result", dedup_key="c-r1")
        check("result turn 2 while root unacked (chain exempt)",
              r1["turn"] == 2 and r1["conversation_id"] == cid, str(r1))
        try:
            server.send_message("kimi", "ещё раз готово", reply_to=root,
                                kind="result", dedup_key="c-r1b")
            check("fork: second result on same task rejected", False)
        except ValueError:
            check("fork: second result on same task rejected", True)
        os.environ["AGENT_ID"] = "kimi"
        try:
            server.send_message("claude", "рез на рез",
                                reply_to=r1["message_id"], kind="result",
                                dedup_key="c-r2")
            check("result-on-result rejected", False)
        except ValueError as exc:
            check("result-on-result rejected",
                  "must answer a 'task'" in str(exc), str(exc))
        r2 = server.send_message("claude", "шаг 2", reply_to=r1["message_id"],
                                 kind="task", dedup_key="k-f1")
        check("reviewer followup turn 3 over unacked chain", r2["turn"] == 3,
              str(r2))
        r2b = server.send_message("claude", "шаг 2", reply_to=r1["message_id"],
                                  kind="task", dedup_key="k-f1")
        check("same dedup = duplicate, no parallel branch",
              r2b.get("duplicate") is True
              and r2b["message_id"] == r2["message_id"], str(r2b))
        os.environ["AGENT_ID"] = "claude"
        r3 = server.send_message("kimi", "готово 2", reply_to=r2["message_id"],
                                 kind="result", dedup_key="c-r3")
        check("conversation_id inherited from reply_to",
              r3["conversation_id"] == cid and r3["turn"] == 4, str(r3))
        try:
            server.send_message("kimi", "x", conversation_id=cid,
                                reply_to=root)
            check("conversation_id without kind rejected", False)
        except ValueError:
            check("conversation_id without kind rejected", True)
        try:
            server.send_message("kimi", "x", conversation_id=cid, kind="task")
            check("conversation task without reply_to rejected", False)
        except ValueError as exc:
            check("conversation task without reply_to rejected",
                  "must reply_to" in str(exc), str(exc))
        os.environ["AGENT_ID"] = "kimi"
        st = server.conversation(action="status", conversation_id=cid)
        blob = json.dumps(st, ensure_ascii=False)
        check("status hides goal text", goal not in blob, blob[:200])
        check("status is state-machine view", st["state"] == "active"
              and st["turn_count"] == 4 and st["in_flight"], str(st)[:200])
    finally:
        os.environ["AGENT_ID"] = "claude"


def test_31_conversation_limits(d: Path):
    # max_turns/expires stop dispatch exactly on the rejected send; terminal
    # states (completed/needs_human/cancelled) admit no new task/result but
    # still accept direction-correct turnless notices.
    server = import_server(d / "server_errors.log")
    db = d / "bridge.db"
    cfg = write_cfg(d)
    set_server_seams(server, d, db, cfg)
    server._init_db()
    os.environ["AGENT_ID"] = "kimi"
    try:
        r = server.start_conversation("claude", "цель", max_turns=2)
        cid, root = r["conversation_id"], r["message_id"]
        os.environ["AGENT_ID"] = "claude"
        r1 = server.send_message("kimi", "готово", reply_to=root,
                                 kind="result", dedup_key="l-r1")
        check("last allowed turn still dispatches", r1["turn"] == 2, str(r1))
        os.environ["AGENT_ID"] = "kimi"
        try:
            server.send_message("claude", "ещё шаг",
                                reply_to=r1["message_id"], kind="task",
                                dedup_key="l-f1")
            check("send over max_turns rejected", False)
        except ValueError:
            check("send over max_turns rejected", True)
        check("turns_exhausted -> needs_human",
              conv_state(db, cid) == "needs_human"
              and "turns_exhausted" in conv_reasons(db, cid),
              str(conv_reasons(db, cid)))
        r = server.start_conversation("claude", "цель")
        cid2, root2 = r["conversation_id"], r["message_id"]
        con = sqlite3.connect(db)
        con.execute("UPDATE conversations SET expires_at=? WHERE id=?",
                    (time.time() - 5, cid2))
        con.commit()
        con.close()
        os.environ["AGENT_ID"] = "claude"
        try:
            server.send_message("kimi", "поздно", reply_to=root2,
                                kind="result", dedup_key="l-r2")
            check("send after expiry rejected", False)
        except ValueError:
            check("send after expiry rejected", True)
        check("expired -> needs_human",
              conv_state(db, cid2) == "needs_human"
              and "expired" in conv_reasons(db, cid2),
              str(conv_reasons(db, cid2)))
        os.environ["AGENT_ID"] = "kimi"
        for action, final in (("complete", "completed"),
                              ("needs_human", "needs_human")):
            r = server.start_conversation("claude", "цель",
                                          dedup_key=f"l-{action}")
            cid3, root3 = r["conversation_id"], r["message_id"]
            server.conversation(action=action, conversation_id=cid3)
            os.environ["AGENT_ID"] = "claude"
            try:
                server.send_message("kimi", "после терминала",
                                    reply_to=root3, kind="result",
                                    dedup_key=f"l-r{action}")
                check(f"{final} blocks task/result", False)
            except ValueError as exc:
                check(f"{final} blocks task/result",
                      f"is {final}" in str(exc), str(exc))
            rn = server.send_message("kimi", "уведомление", reply_to=root3,
                                     kind="notice", dedup_key=f"l-n{action}")
            check("notice allowed in terminal state, turn NULL",
                  conv_cols(db, rn["message_id"]) == (cid3, "notice", None),
                  str(conv_cols(db, rn["message_id"])))
            check("notice keeps state and turn budget",
                  conv_state(db, cid3) == final
                  and conv_turn(db, cid3) == 1,
                  f"{conv_state(db, cid3)} {conv_turn(db, cid3)}")
            os.environ["AGENT_ID"] = "kimi"
            try:
                server.send_message("claude", "не та сторона",
                                    reply_to=root3, kind="notice",
                                    dedup_key=f"l-nb{action}")
                check("notice from wrong side rejected", False)
            except ValueError:
                check("notice from wrong side rejected", True)
        r = server.start_conversation("claude", "цель", dedup_key="l-cancel")
        cid4, root4 = r["conversation_id"], r["message_id"]
        cc = server.conversation(action="cancel", conversation_id=cid4)
        check("cancel -> cancelled", cc["state"] == "cancelled", str(cc))
        os.environ["AGENT_ID"] = "claude"
        try:
            server.send_message("kimi", "отменено", reply_to=root4,
                                kind="result", dedup_key="l-r4")
            check("cancelled blocks dispatch", False)
        except ValueError as exc:
            check("cancelled blocks dispatch", "is cancelled" in str(exc),
                  str(exc))
        os.environ["AGENT_ID"] = "kimi"
        c2 = server.conversation(action="cancel", conversation_id=cid4)
        check("repeat cancel is idempotent", c2["state"] == "cancelled")
    finally:
        os.environ["AGENT_ID"] = "claude"


def test_32_blocked_flow_inflight(d: Path):
    # blocked worker = result sent, claim released, nothing acked: the
    # reviewer's followup and the next worker result must still pass,
    # because the whole answered chain is the single thread, not a fork.
    server = import_server(d / "server_errors.log")
    db = d / "bridge.db"
    cfg = write_cfg(d)
    set_server_seams(server, d, db, cfg)
    server._init_db()
    os.environ["AGENT_ID"] = "kimi"
    try:
        r = server.start_conversation("claude", "цель", max_turns=6)
        cid, root = r["conversation_id"], r["message_id"]
        os.environ["AGENT_ID"] = "claude"
        r1 = server.send_message("kimi", "blocked: нет доступа",
                                 reply_to=root, kind="result",
                                 dedup_key="b-r1")
        con = sqlite3.connect(db)
        con.execute("UPDATE messages SET claimed_by=NULL,"
                    " claim_expires_at=NULL WHERE id=?", (root,))
        con.commit()
        con.close()
        os.environ["AGENT_ID"] = "kimi"
        f1 = server.send_message("claude", "сделай иначе",
                                 reply_to=r1["message_id"], kind="task",
                                 dedup_key="b-f1")
        check("followup over unacked root+result passes", f1["turn"] == 3,
              str(f1))
        os.environ["AGENT_ID"] = "claude"
        r2 = server.send_message("kimi", "готово", reply_to=f1["message_id"],
                                 kind="result", dedup_key="b-r2")
        check("second-cycle result passes", r2["turn"] == 4, str(r2))
        os.environ["AGENT_ID"] = "kimi"
        try:
            server.send_message("claude", "ветка", reply_to=r1["message_id"],
                                kind="task", dedup_key="b-f1b")
            check("fork off the answered result rejected", False)
        except ValueError:
            check("fork off the answered result rejected", True)
        os.environ["AGENT_ID"] = "claude"
        try:
            server.send_message("kimi", "продублирую root", reply_to=root,
                                kind="result", dedup_key="b-r1b")
            check("fork on the answered root rejected", False)
        except ValueError:
            check("fork on the answered root rejected", True)
    finally:
        os.environ["AGENT_ID"] = "claude"


def test_33_supervisor_conversation_task(d: Path):
    # The supervisor dispatches a conversation task exactly like a legacy
    # root task; the worker's result must carry the conversation link.
    db = new_db(d)
    cfg = write_cfg(d)
    cid, mid = seed_conv(db)
    r = sup_run(d, db, cfg, mid, "--auto")
    check("rc0", r.returncode == 0, r.stderr[-400:])
    check("attempt succeeded", attempt_row(db, mid)[0] == "succeeded",
          str(attempt_row(db, mid)))
    rr = result_row(db, mid, "cclocal", "claude", "kimi")
    check("result row present", rr is not None)
    if rr is not None:
        check("result linked to conversation as turn 2",
              conv_cols(db, rr[0]) == (cid, "result", 2),
              str(conv_cols(db, rr[0])))
    check("turn_count bumped to 2", conv_turn(db, cid) == 2)
    check("conversation stays active (worker does not steer state)",
          conv_state(db, cid) == "active")
    check("no notice", notice_row(db, mid) is None)


def test_34_blocked_then_needs_human(d: Path):
    # blocked worker: conversation stays active (blocked is a protocol
    # outcome, not a dispatch failure); the reviewer then freezes it via
    # needs_human and the supervisor counts the review as succeeded.
    db = new_db(d)
    cfg = write_cfg(d, kimi=True)
    cid, mid = seed_conv(db)
    r = sup_run(d, db, cfg, mid, "--auto", mode="blocked")
    check("attempt blocked", attempt_row(db, mid)[0] == "blocked",
          str(attempt_row(db, mid)))
    check("no freeze on blocked", conv_state(db, cid) == "active")
    check("no notice on blocked", notice_row(db, mid) is None)
    rr = result_row(db, mid, "cclocal", "claude", "kimi")
    check("blocked result linked", rr is not None
          and rr[1].startswith("blocked:")
          and conv_cols(db, rr[0]) == (cid, "result", 2), str(rr))
    r2 = sup_run(d, db, cfg, rr[0], "--auto", agent="kimi",
                 mode="review_needs_human")
    check("review rc0", r2.returncode == 0, r2.stderr[-400:])
    check("reviewer succeeded", attempt_row(db, rr[0])[0] == "succeeded",
          str(attempt_row(db, rr[0])))
    check("conversation needs_human", conv_state(db, cid) == "needs_human")
    check("reviewed result acked", q1(db, "SELECT acknowledged FROM messages"
                                      " WHERE id=?", rr[0])[0] == 1)


def test_35_hard_failure_freezes_conversation(d: Path):
    # crash/uncertain: conversation is frozen 'blocked' with a
    # dispatch_failed transition and exactly one conversation notice;
    # rerun of the same message dispatches nothing.
    db = new_db(d)
    cfg = write_cfg(d, kimi=True)
    cid, mid = seed_conv(db)
    r = sup_run(d, db, cfg, mid, "--auto", mode="crash")
    check("rc3", r.returncode == 3)
    check("attempt failed", attempt_row(db, mid)[0] == "failed")
    check("conversation blocked", conv_state(db, cid) == "blocked")
    check("dispatch_failed recorded",
          "dispatch_failed" in conv_reasons(db, cid),
          str(conv_reasons(db, cid)))
    n = q1(db, "SELECT conversation_id, kind, turn FROM messages WHERE"
               " dedup_key=?", f"cclocal:{mid}:dispatch-error")
    check("notice linked to conversation, turnless",
          n is not None and n[0] == cid and n[1] == "notice"
          and n[2] is None, str(n))
    sup_run(d, db, cfg, mid, "--auto", mode="crash")
    check("no rerun after failure",
          runs_of(d) == [f"RAN {mid} via=stdin agent=claude"], str(runs_of(d)))
    check("exactly one notice",
          q1(db, "SELECT COUNT(*) FROM messages WHERE kind='notice'")[0] == 1)
    cid2, mid2 = seed_conv(db, initiator="claude", recipient="kimi")
    r2 = sup_run(d, db, cfg, mid2, "--auto", mode="silent0", agent="kimi")
    check("silent0 uncertain", attempt_row(db, mid2)[0] == "uncertain",
          str(attempt_row(db, mid2)))
    check("uncertain also freezes", conv_state(db, cid2) == "blocked")


def test_41_result_on_result_uncertain(d: Path):
    # A misbehaving reviewer that answers a conversation result with a plain
    # result: role-appropriate evidence (followup/terminal) is missing, so
    # the supervisor must report uncertain — never success — and freeze.
    db = new_db(d)
    cfg = write_cfg(d, kimi=True)
    cid, root = seed_conv(db)
    r1 = conv_msg(db, cid, "claude", "kimi", root, "result",
                  dedup="ro-r1")
    sup_run(d, db, cfg, r1, "--auto", agent="kimi", mode="success")
    check("not classified as success", attempt_row(db, r1)[0] == "uncertain",
          str(attempt_row(db, r1)))
    check("conversation frozen blocked", conv_state(db, cid) == "blocked")
    check("notice sent", notice_row(db, r1, "kimi") is not None)


def test_42_kimi_gate_blocks_review(d: Path):
    # auto.kimi=false (the shipped default): a conversation result for kimi
    # gets no review worker at all — the dialogue halts at that turn until
    # the owner flips the gate. Both gate states are implemented.
    db = new_db(d)
    cfg = write_cfg(d)                     # kimi: False
    cid, root = seed_conv(db)
    r1 = conv_msg(db, cid, "claude", "kimi", root, "result", dedup="g-r1")
    r = sup_run(d, db, cfg, r1, "--auto", agent="kimi",
                mode="review_complete")
    check("gate skip rc0", r.returncode == 0, r.stderr[-400:])
    check("no attempt", attempt_row(db, r1) is None)
    check("no runs", runs_of(d) == [])
    check("conversation untouched", conv_state(db, cid) == "active")


def test_39_legacy_schema_fallback(d: Path):
    # A bridge.db from before conversations: supervisor, notify guard and
    # listener scan must keep working through their guarded fallbacks.
    db = new_db(d, legacy=True)
    cfg = write_cfg(d)
    cols = [r[1] for r in sqlite3.connect(db).execute(
        "PRAGMA table_info(messages)")]
    check("legacy shape", "conversation_id" not in cols and "kind" not in cols,
          str(cols))
    mid = seed(db)
    r = sup_run(d, db, cfg, mid, "--auto")
    check("supervisor dispatches on legacy db", r.returncode == 0
          and attempt_row(db, mid)[0] == "succeeded", r.stderr[-400:])
    check("legacy worker ran once",
          runs_of(d) == [f"RAN {mid} via=stdin agent=claude"], str(runs_of(d)))
    r1 = sup_run(d, db, cfg, "--recover")
    check("recover rc0 on legacy db", r1.returncode == 0, r1.stderr[-400:])
    mid2 = seed(db)
    r2 = sup_run(d, db, cfg, mid2, "--auto", mode="crash")
    check("crash rc3", r2.returncode == 3)
    check("notice written via legacy guard", notice_row(db, mid2) is not None)
    rt = d / "runtime2"
    p = spawn_listener("claude", rt, db=db, cfg=cfg,
                       on_message=FAKE_SUPERVISOR, env=fake_sup_env(d))
    try:
        mid3 = seed(db)
        check("listener scans legacy root via fallback", wait_for(
            lambda: len(supervisor_log_lines(d)) == 1, timeout=10.0))
        check("legacy root dispatched",
              supervisor_log_lines(d)
              and supervisor_log_lines(d)[0].startswith(f"{mid3} --db"),
              str(supervisor_log_lines(d)))
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_40_bounded_history(d: Path):
    # {{CONTEXT}} shows the goal and only the newest HISTORY_LIMIT items,
    # per-item truncated; the current message stays whole; the role protocol
    # differs for worker and reviewer; the log stays payload-free.
    db = new_db(d)
    cfg = write_cfg(d)
    cid, root = seed_conv(db, goal="СОБРАТЬ ОТЧЁТ 42")
    old_body = "САМОЕ-СТАРОЕ-" + "x" * 1500
    prev = root
    for i in range(14):
        body = f"{old_body if i == 0 else 'сообщение'} {i} " + "y" * 1300
        if i == 13:
            body = "САМОЕ-СВЕЖЕЕ-9c4a " + body
        prev = conv_msg(db, cid, "kimi" if i % 2 else "claude",
                        "claude" if i % 2 else "kimi", prev,
                        "result" if i % 2 else "task", text=body,
                        dedup=f"hist-{i}")
    dump1 = d / "prompt_review.txt"
    r = sup_run(d, db, cfg, prev, "--auto", mode="dump",
                extra_env={"FAKE_DUMP": str(dump1)})
    # a dumped prompt never completes the protocol, so the run itself is
    # classified uncertain — the artifact under test is the rendered file
    check("reviewer prompt rendered", dump1.exists(), r.stderr[-400:])
    t1 = dump1.read_text(errors="replace")
    check("context block present",
          "=== Контекст автономного диалога" in t1)
    check("goal visible to worker", "СОБРАТЬ ОТЧЁТ 42" in t1)
    check("history bounded to 12 items", t1.count("[id ") == 12,
          str(t1.count("[id ")))
    check("oldest dropped", "САМОЕ-СТАРОЕ" not in t1)
    check("current message text kept whole", "САМОЕ-СВЕЖЕЕ" in t1)
    check("per-item truncation marker", "…" in t1)
    check("reviewer role and protocol", "ревьюер результата" in t1
          and "РОВНО ОДИН" in t1)
    cid2, root2 = seed_conv(db, goal="ВТОРАЯ ЦЕЛЬ 7")
    dump2 = d / "prompt_task.txt"
    r2 = sup_run(d, db, cfg, root2, "--auto", mode="dump",
                 extra_env={"FAKE_DUMP": str(dump2)})
    t2 = dump2.read_text(errors="replace")
    check("worker role and protocol", "исполнитель задачи" in t2
          and "2. Выполни задачу в рамках цели диалога" in t2,
          r2.stderr[-300:])
    check("legacy protocol text absent for conversation task",
          "запускаются только корневые задачи" not in t2)
    log = d / "runtime" / "dispatcher.log"
    blob = log.read_text(errors="replace") if log.exists() else ""
    check("dispatcher.log payload-free", SENTINEL not in blob)


def _dialogue_e2e(d: Path, initiator: str):
    # Full autonomous cycle on both listeners with the real on_message and
    # scripted fake workers: goal -> task -> result -> followup task ->
    # result -> completed, with no manual phrase anywhere in between.
    other = "claude" if initiator == "kimi" else "kimi"
    db = new_db(d)
    cfg = write_cfg(d, claude=True, kimi=True)
    rt_om = d / "runtime"
    om = real_om_wrapper(d, rt_om)
    wscript = d / "worker.script"
    wscript.write_text("success success", encoding="utf-8")
    rscript = d / "review.script"
    rscript.write_text("review_followup review_complete", encoding="utf-8")

    def wenv(script: Path, agent: str) -> dict:
        return listener_env(FAKE_PY=PY, FAKE_WORKER=str(FAKE_WORKER),
                            FAKE_MODE="script", FAKE_SCRIPT=str(script),
                            FAKE_DB=str(db), FAKE_RUNS=str(d / "runs.txt"),
                            FAKE_AGENT=agent)

    server = import_server(d / "server_errors.log")
    set_server_seams(server, d, db, cfg)
    rt_w = d / "rt_w"
    rt_r = d / "rt_r"
    p_w = p_r = None
    os.environ["AGENT_ID"] = initiator
    try:
        p_w = spawn_listener(other, rt_w, db=db, cfg=cfg, on_message=om,
                             env=wenv(wscript, other))
        p_r = spawn_listener(initiator, rt_r, db=db, cfg=cfg, on_message=om,
                             env=wenv(rscript, initiator))
        check("both listeners up", wait_for(
            lambda: listener_status(other, rt_w, cfg)["running"]
            and listener_status(initiator, rt_r, cfg)["running"],
            timeout=10.0))
        r = server.start_conversation(other, "собрать отчёт", max_turns=6,
                                      response_timeout_seconds=600)
        cid = r["conversation_id"]
        ok = wait_for(lambda: conv_state(db, cid) == "completed",
                      timeout=30.0)
        check("dialogue completed autonomously", ok,
              f"state={conv_state(db, cid)}")
        check("four turns used", conv_turn(db, cid) == 4,
              str(conv_turn(db, cid)))
        check("exactly four messages in conversation",
              q1(db, "SELECT COUNT(*) FROM messages WHERE conversation_id=?"
                 , cid)[0] == 4)
        check("four dispatched messages total",
              q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0] == 4,
              str(q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0]))
        rans = runs_of(d)
        check("worker side ran exactly twice",
              len([ln for ln in rans if f"agent={other}" in ln]) == 2,
              str(rans))
        check("reviewer side ran exactly twice",
              len([ln for ln in rans if f"agent={initiator}" in ln]) == 2,
              str(rans))
        check("no notices in a clean dialogue",
              q1(db, "SELECT COUNT(*) FROM messages WHERE kind='notice'")[0]
              == 0)
        time.sleep(2.0)
        check("nothing ignites after completion", len(runs_of(d)) == 4,
              str(runs_of(d)))
    finally:
        os.environ["AGENT_ID"] = "claude"
        stop_listener(other, rt_w, cfg, proc=p_w)
        stop_listener(initiator, rt_r, cfg, proc=p_r)


def test_36_dialogue_e2e_kimi_initiator(d: Path):
    _dialogue_e2e(d, "kimi")


def test_37_dialogue_e2e_claude_initiator(d: Path):
    _dialogue_e2e(d, "claude")


def test_38_listener_skips_dead_conversations(d: Path):
    # Dead conversation states and expiry must not dispatch; the expired
    # sweep freezes active-but-expired once and idempotently; a live task
    # dispatches exactly once even with two listeners on the same DB (the
    # real on_message attempt-PK is the barrier); a notice wakes nobody.
    db = new_db(d)
    cfg = write_cfg(d)
    rt1 = d / "rt1"
    rt2 = d / "rt2"
    om = real_om_wrapper(d, d / "runtime")
    env = fake_worker_env(d, db, mode="success")
    p1 = p2 = None
    try:
        p1 = spawn_listener("claude", rt1, db=db, cfg=cfg, on_message=om,
                            env=env)
        p2 = spawn_listener("claude", rt2, db=db, cfg=cfg, on_message=om,
                            env=env)
        check("both listeners up", wait_for(
            lambda: listener_status("claude", rt1, cfg)["running"]
            and listener_status("claude", rt2, cfg)["running"],
            timeout=10.0))
        c_done, _ = seed_conv(db)
        c_nh, _ = seed_conv(db)
        c_can, _ = seed_conv(db)
        c_exp, _ = seed_conv(db, expires_in=-10.0)
        con = sqlite3.connect(db)
        for cid_s, st_s in ((c_done, "completed"), (c_nh, "needs_human"),
                            (c_can, "cancelled")):
            con.execute("UPDATE conversations SET state=? WHERE id=?",
                        (st_s, cid_s))
        con.commit()
        con.close()
        time.sleep(2.5)
        check("dead conversations never dispatch", runs_of(d) == [],
              str(runs_of(d)))
        check("expired active swept to needs_human",
              conv_state(db, c_exp) == "needs_human"
              and "expired" in conv_reasons(db, c_exp),
              str(conv_reasons(db, c_exp)))
        check("terminal states kept by sweep",
              conv_state(db, c_done) == "completed"
              and conv_state(db, c_nh) == "needs_human"
              and conv_state(db, c_can) == "cancelled")
        live, root_live = seed_conv(db)
        check("live conversation task dispatched across two listeners",
              wait_for(lambda: len(runs_of(d)) >= 1, timeout=12.0))
        time.sleep(2.0)
        check("exactly one dispatch, the live root",
              runs_of(d) == [f"RAN {root_live} via=stdin agent=claude"],
              str(runs_of(d)))
        turn_before = conv_turn(db, live)
        conv_msg(db, live, "kimi", "claude", root_live, "notice",
                 text="уведомление", bump_turns=False)
        time.sleep(2.0)
        check("notice does not wake a worker", len(runs_of(d)) == 1,
              str(runs_of(d)))
        check("notice consumed no turn", conv_turn(db, live) == turn_before)
    finally:
        stop_listener("claude", rt1, cfg, proc=p1)
        stop_listener("claude", rt2, cfg, proc=p2)


# ------------------------------------------- fresh DB, blocked evidence, progress

def test_43_fresh_db_listener_end_to_end_dispatch(d: Path):
    # The live bootstrap bug: a database that has never run a supervisor has
    # no dispatch_attempts table; the scan read that as "nothing to do", and
    # on_message.py — the only thing that used to create the table — runs only
    # AFTER a scan found work. The listener now ensures the schema itself,
    # additively, before its first scan. Nothing here calls on_message by hand.
    db = new_db(d, attempts=False)
    cfg = write_cfg(d)
    rt = d / "runtime"

    def attempt_now(mid: int):
        try:
            return attempt_row(db, mid)
        except sqlite3.OperationalError:   # the table may not exist yet
            return None

    check("database starts without dispatch_attempts",
          q1(db, "SELECT 1 FROM sqlite_master WHERE type='table'"
                 " AND name='dispatch_attempts'") is None)
    om = real_om_wrapper(d, rt)
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db))
    try:
        mid = seed(db)
        ok = wait_for(lambda: (attempt_now(mid) or ("",))[0] == "succeeded",
                      timeout=20.0)
        check("first ever task dispatched with no manual supervisor run",
              ok, str(attempt_now(mid)))
        check("listener created the table",
              q1(db, "SELECT 1 FROM sqlite_master WHERE type='table'"
                     " AND name='dispatch_attempts'") is not None)
        check("original acknowledged",
              (q1(db, "SELECT acknowledged FROM messages WHERE id=?", mid)
               or (0,))[0] == 1)
        check("result message stored",
              result_row(db, mid, "cclocal", "claude", "kimi") is not None)
        check("no failure notice", notice_row(db, mid) is None)
        log = (rt / "dispatcher.log").read_text(errors="replace") \
            if (rt / "dispatcher.log").exists() else ""
        check("creation of the table is logged",
              "dispatch_attempts created" in log, log[-300:])
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_44_schema_ensure_preserves_existing_rows(d: Path):
    # Additive against a live database: rows already in dispatch_attempts —
    # the auto-retry barrier itself — survive the listener's ensure untouched,
    # the column shape is unchanged, and new work still flows beside them.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    now = time.time()
    old = seed(db)
    insert_attempt(db, old, "failed", None, now - 600)
    om = real_om_wrapper(d, rt)
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db))
    try:
        mid = seed(db)
        ok = wait_for(lambda: (attempt_row(db, mid) or ("",))[0]
                      == "succeeded", timeout=20.0)
        check("new task dispatched beside the old attempt", ok,
              str(attempt_row(db, mid)))
        row = q1(db, "SELECT state, started_at, pid, exit_code, error_kind"
                     " FROM dispatch_attempts WHERE message_id=?", old)
        check("pre-existing attempt row untouched",
              row is not None and row[0] == "failed"
              and abs(row[1] - (now - 600)) < 1e-6
              and row[2] is None and row[3] is None and row[4] is None,
              str(row))
        con = sqlite3.connect(db)
        try:
            cols = [r[1] for r in con.execute(
                "PRAGMA table_info(dispatch_attempts)").fetchall()]
        finally:
            con.close()
        check("canonical column shape kept",
              cols == ["message_id", "state", "started_at", "finished_at",
                       "pid", "exit_code", "error_kind"], str(cols))
        check("both attempt rows present",
              q1(db, "SELECT COUNT(*) FROM dispatch_attempts")[0] == 2)
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_45_progress_stages_sequence(d: Path):
    # What progress.json must show while work is in flight, and what it must
    # leave behind: supervising (with a live supervisor pid) observable during
    # the dispatch, current cleared when it ends, and the finished history
    # still readable after the listener itself is gone. The 'received' stage
    # lives between two writes, far too short to sample from outside — so the
    # fake supervisor snapshots the file at its own start: the child cannot
    # exist before the listener wrote it.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    env = listener_env(FAKE_SUP_LOG=str(d / "sup.log"),
                       FAKE_SUP_PROTOCOL="1", FAKE_SUP_AGENT="claude",
                       FAKE_SUP_DELAY="4",
                       FAKE_SUP_SEE_PROGRESS=str(rt / "claude.progress.json"))
    p = spawn_listener("claude", rt, db=db, cfg=cfg,
                       on_message=FAKE_SUPERVISOR, env=env)
    try:
        mid = seed(db)
        ok = wait_for(
            lambda: (listener_status("claude", rt, cfg).get("progress")
                     or {}).get("stage") == "supervising", timeout=12.0)
        cur = listener_status("claude", rt, cfg).get("progress") or {}
        check("stage supervising observed", ok, str(cur))
        sup_pid = cur.get("supervisor_pid")
        check("supervising carries a live supervisor pid",
              sup_pid is not None and pid_alive(sup_pid), str(cur))
        seen = None
        sf = rt / "claude.progress.json.seen"
        if wait_for(lambda: sf.exists(), timeout=6.0):
            seen = json.loads(sf.read_text(encoding="utf-8"))
        check("progress existed at the spawn (received before Popen)",
              isinstance(seen, dict) and seen.get("message") == mid
              and seen.get("stage") in ("received", "supervising"),
              str(seen))
        ok = wait_for(
            lambda: (listener_status("claude", rt, cfg).get("last")
                     or {}).get("stage") == "finished", timeout=15.0)
        last = listener_status("claude", rt, cfg).get("last") or {}
        check("dispatch closes as finished", ok, str(last))
        check("finished names the message and the supervisor rc",
              last.get("message") == mid and last.get("supervisor_rc") == 0,
              str(last))
        check("no verdict invented without a dispatch_attempts row",
              "result" not in last, str(last))
        st = listener_status("claude", rt, cfg)
        check("current cleared and idle once finished",
              st["progress"] is None and st["stage"] == "idle", str(st))
    finally:
        stop_listener("claude", rt, cfg, proc=p)
    check("dead listener reads stopped", wait_for(
        lambda: listener_status("claude", rt, cfg)["stage"] == "stopped",
        timeout=8.0))
    check("history survives the listener",
          (listener_status("claude", rt, cfg).get("last") or {}).get(
              "message") == mid)


def test_46_progress_terminal_and_stale_recovery(d: Path):
    # SIGKILL of our own listener mid-dispatch: the open 'current' must not
    # masquerade as live progress (dead pid => 'stopped'), and the next
    # listener must close it in history as 'interrupted' and then sit idle.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    # 10 s worker timeout keeps the supervising window wide enough to observe
    # the in-flight stage and SIGKILL the listener inside it.
    om = real_om_wrapper(d, rt, wtimeout="10")
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db, mode="sleep"))
    p2 = None
    try:
        mid = seed(db)
        ok = wait_for(
            lambda: (listener_status("claude", rt, cfg).get("progress")
                     or {}).get("stage") == "supervising", timeout=15.0)
        check("dispatch is in flight", ok)
        pid = listener_status("claude", rt, cfg)["pid"]
        check("listener pid verified via /proc and alive",
              pid is not None and pid_alive(pid))
        os.kill(pid, signal.SIGKILL)
        check("killed listener disappears", wait_for(
            lambda: listener_status("claude", rt, cfg)["running"] is False,
            timeout=8.0))
        p.wait(timeout=5)
        st = listener_status("claude", rt, cfg)
        check("dead listener: stopped, nothing trusted as current",
              st["stage"] == "stopped" and st["progress"] is None, str(st))
        doc = json.loads((rt / "claude.progress.json").read_text(
            encoding="utf-8"))
        cur = doc.get("current") or {}
        check("the raw file still shows the interrupted dispatch",
              cur.get("message") == mid and cur.get("stage") == "supervising",
              str(cur))
        p2 = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                            env=fake_worker_env(d, db, mode="sleep"))
        ok = wait_for(
            lambda: (listener_status("claude", rt, cfg).get("last")
                     or {}).get("result") == "interrupted", timeout=12.0)
        check("restart closes the leftover as interrupted", ok,
              str(listener_status("claude", rt, cfg).get("last")))
        st = listener_status("claude", rt, cfg)
        check("restarted listener reads idle",
              st["stage"] == "idle" and st["progress"] is None, str(st))
        time.sleep(2.5)
        check("stale recovery is idempotent", (listener_status(
            "claude", rt, cfg).get("last") or {}).get("result")
            == "interrupted")
        rans = [ln for ln in runs_of(d) if f"RAN {mid}" in ln]
        check("the killed dispatch is never re-run", len(rans) <= 1,
              str(rans))
    finally:
        stop_listener("claude", rt, cfg, proc=p2 if p2 is not None else p)


def test_47_status_backcompat_without_progress_file(d: Path):
    # A listener still running OLD code: /proc proves its pid, but it never
    # writes progress.json. status() keeps every legacy key intact and answers
    # 'unknown' — honest invisibility — instead of inventing a stage.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    rt.mkdir(parents=True, exist_ok=True)
    # 'watch' is a real listener.py invocation that _listener_pid() accepts
    # but that never dispatches and never writes progress: the stand-in for a
    # listener built before this feature existed.
    w = subprocess.Popen(listener_cmd("watch", "claude", rt, cfg=cfg),
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, env=listener_env())
    try:
        (rt / "claude.listener.pid").write_text(f"{w.pid}\n")
        (rt / "claude.listener.state").write_text("waiting_worker:999\n")
        st = listener_status("claude", rt, cfg)
        check("legacy keys unchanged",
              st["agent"] == "claude" and st["enabled"] is True
              and st["running"] is True and st["pid"] == w.pid
              and st["message"] == 999 and st["auto"] is True, str(st))
        check("no invented stage", st["stage"] == "unknown", str(st))
        check("progress hidden when absent", st["progress"] is None)
        check("no history yet", st["last"] is None)
        pv = listener_cli("progress", "claude", rt, db=db, cfg=cfg)
        check("progress CLI works without a progress file",
              pv.returncode == 0 and "[claude] unknown msg#999" in pv.stdout,
              pv.stdout)
        check("no sender invented for an id the database does not know",
              "sender=" not in pv.stdout, pv.stdout)
        v = subprocess.run(
            listener_cmd("watch", "claude", rt, db=db, cfg=cfg) + ["--once"],
            capture_output=True, text=True, timeout=10, env=listener_env())
        check("watch --once prints one snapshot and exits",
              v.returncode == 0 and v.stdout.count("\n") == 1, v.stdout)
    finally:
        w.terminate()
        try:
            w.wait(timeout=5)
        except subprocess.TimeoutExpired:
            w.kill()


def test_48_progress_and_status_no_payload(d: Path):
    # The progress channel is readable by any process and may outlive a crash:
    # it must carry ids, enums and numbers only. Task text must not reach the
    # progress file, the listener log, or either human-readable surface.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    om = real_om_wrapper(d, rt)
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db))
    try:
        mid = seed(db)                       # text carries SENTINEL
        ok = wait_for(lambda: (attempt_row(db, mid) or ("",))[0]
                      == "succeeded", timeout=20.0)
        check("dispatch finished for the assertions", ok)
        doc = (rt / "claude.progress.json").read_text(errors="replace")
        check("progress file free of payload", SENTINEL not in doc,
              doc[-300:])
        log = (rt / "dispatcher.log").read_text(errors="replace") \
            if (rt / "dispatcher.log").exists() else ""
        check("dispatcher log free of payload", SENTINEL not in log)
        st = listener_cli("status", "claude", rt, cfg=cfg)
        check("status output free of payload",
              st.returncode == 0 and SENTINEL not in st.stdout,
              st.stdout[-300:])
        pv = listener_cli("progress", "claude", rt, cfg=cfg)
        check("progress line free of payload",
              pv.returncode == 0 and SENTINEL not in pv.stdout, pv.stdout)
        last = json.loads(st.stdout).get("last") or {}
        check("finished verdict read from dispatch_attempts, not invented",
              last.get("result") == "succeeded" and last.get("message") == mid,
              str(last))
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_49_blocked_report_alt_dedup_key(d: Path):
    # The live task-27 bug: the worker stopped honestly but filed the blocker
    # under its own '<prefix>:<id>:blocked' key instead of the prescribed
    # ':result'. That must classify 'blocked' — not 'uncertain' — so: no
    # dispatch-error notice, no frozen dialogue, supervisor rc 0.
    db = new_db(d)
    cfg = write_cfg(d)
    mid = seed(db)
    r = sup_run(d, db, cfg, mid, "--auto", mode="blocked_alt_key")
    check("rc0 (the bug produced rc 3 via uncertain)",
          r.returncode == 0, f"rc={r.returncode} {r.stderr[-300:]}")
    check("honest blocked state", attempt_row(db, mid)[0] == "blocked",
          str(attempt_row(db, mid)))
    check("no dispatch-error notice", notice_row(db, mid) is None)
    check("original stays unacked",
          q1(db, "SELECT acknowledged FROM messages WHERE id=?", mid)[0] == 0)
    row = q1(db, "SELECT text FROM messages WHERE dedup_key=?",
             f"cclocal:{mid}:blocked")
    check("report exists under the blocked key",
          row is not None and "blocked" in row[0], str(row))
    check("nothing under the prescribed result key",
          result_row(db, mid, "cclocal", "claude", "kimi") is None)


def test_50_blocked_report_reconcile_recover(d: Path):
    # --recover must classify by the same evidence: a supervisor that died
    # while its worker had honestly stopped reconciles to 'blocked', not
    # 'interrupted' — the blocked report is provable, nothing crashed.
    db = new_db(d)
    cfg = write_cfg(d)
    now = time.time()
    mid = seed(db)
    insert_attempt(db, mid, "spawned", dead_pid(), now - 3600)
    con = sqlite3.connect(db)
    con.execute("PRAGMA busy_timeout=5000")
    con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to, created_at,"
        " response_deadline, dedup_key) VALUES (?,?,?,?,?,?,?)",
        ("claude", "kimi", "blocked: тест-причина", mid, now, now + 60,
         f"cclocal:{mid}:blocked"))
    con.commit()
    con.close()
    r = sup_run(d, db, cfg, "--recover")
    check("recover rc0", r.returncode == 0, r.stderr[-500:])
    check("reconcile accepts a blocked-key report",
          attempt_row(db, mid)[0] == "blocked", str(attempt_row(db, mid)))
    check("no notice for a provable stop", notice_row(db, mid) is None)
    check("stale attempt classified, never re-run", runs_of(d) == [],
          str(runs_of(d)))


def test_51_progress_line_format_contract(d: Path):
    # The statusline line contract, pinned end-to-end while a dispatch is in
    # flight and after it closes: one line, stage word, msg# followed by the
    # sender (a database lookup of ids and names — the SENTINEL task text must
    # not reach the line), live supervisor pid and elapsed time while running,
    # then the history segment with duration and rc. The fake supervisor files
    # no dispatch_attempts row, so the honest verdict word stays '?'.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    env = listener_env(FAKE_SUP_LOG=str(d / "sup.log"),
                       FAKE_SUP_PROTOCOL="1", FAKE_SUP_AGENT="claude",
                       FAKE_SUP_DELAY="4")
    p = spawn_listener("claude", rt, db=db, cfg=cfg,
                       on_message=FAKE_SUPERVISOR, env=env)
    try:
        mid = seed(db)
        ok = wait_for(
            lambda: (listener_status("claude", rt, cfg).get("progress")
                     or {}).get("stage") == "supervising", timeout=12.0)
        check("dispatch is in flight", ok)
        pv = listener_cli("progress", "claude", rt, db=db, cfg=cfg)
        line = pv.stdout.strip()
        check("in-flight output rc0 and exactly one line",
              pv.returncode == 0 and pv.stdout.count("\n") == 1, repr(pv.stdout))
        check("in-flight line matches the contract",
              re.fullmatch(rf"\[claude\] supervising msg#{mid} sender=kimi"
                           r" supervisor=\d+ for \d+s", line), line)
        check("no payload in the line", SENTINEL not in line, line)
        ok = wait_for(
            lambda: (listener_status("claude", rt, cfg).get("last")
                     or {}).get("stage") == "finished", timeout=15.0)
        check("dispatch closed", ok)
        line = listener_cli("progress", "claude", rt, db=db,
                            cfg=cfg).stdout.strip()
        check("history segment carries sender, verdict and duration/rc",
              re.fullmatch(rf"\[claude\] idle \| last msg#{mid} sender=kimi"
                           r" result=\? in \d+\.\d+s rc=0", line), line)
    finally:
        stop_listener("claude", rt, cfg, proc=p)


def test_52_progress_line_history_and_robust(d: Path):
    # Two faces of the same line: after a real dispatch the history segment
    # must show the supervisor's true verdict from dispatch_attempts; against
    # garbage (mistyped --db, corrupt database, junk progress.json, malformed
    # config) it must degrade, never raise — rc 0 always, and a mistyped path
    # must not CREATE a stray database (sqlite creates on connect).
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    om = real_om_wrapper(d, rt)
    p = spawn_listener("claude", rt, db=db, cfg=cfg, on_message=om,
                       env=fake_worker_env(d, db))
    try:
        mid = seed(db)                       # text carries SENTINEL
        ok = wait_for(lambda: (attempt_row(db, mid) or ("",))[0]
                      == "succeeded", timeout=20.0)
        check("dispatch finished for the assertions", ok)
        line = listener_cli("progress", "claude", rt, db=db,
                            cfg=cfg).stdout.strip()
        check("succeeded verdict rendered from dispatch_attempts",
              re.fullmatch(rf"\[claude\] idle \| last msg#{mid} sender=kimi"
                           r" result=succeeded in \d+\.\d+s rc=0", line), line)
        check("no payload in the line", SENTINEL not in line, line)
        check("graceful stop", stop_listener("claude", rt, cfg, proc=p))
        p = None
        # A live-but-progress-blind listener (test_47's stand-in) so the
        # state-file message id keeps the database lookup on the hot path.
        # The stop left a disable marker and a trusted-by-nobody progress
        # document behind; clear both so the expected lines stay exact.
        (rt / "claude.disabled").unlink(missing_ok=True)
        (rt / "claude.progress.json").unlink(missing_ok=True)
        w = subprocess.Popen(listener_cmd("watch", "claude", rt, cfg=cfg),
                             stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, env=listener_env())
        try:
            (rt / "claude.listener.pid").write_text(f"{w.pid}\n")
            (rt / "claude.listener.state").write_text("waiting_worker:999\n")
            typo = d / "no-such-dir" / "typo.db"
            pv = listener_cli("progress", "claude", rt, db=typo, cfg=cfg)
            check("mistyped --db: rc0, id kept, no sender",
                  pv.returncode == 0
                  and pv.stdout.strip() == "[claude] unknown msg#999",
                  f"rc={pv.returncode} out={pv.stdout!r}")
            check("mistyped --db creates no stray database",
                  not typo.exists())
            junk = d / "junk.db"
            junk.write_bytes(b"this is not a sqlite database" * 40)
            pv = listener_cli("progress", "claude", rt, db=junk, cfg=cfg)
            check("corrupt database: rc0, sender degrades away",
                  pv.returncode == 0
                  and pv.stdout.strip() == "[claude] unknown msg#999",
                  f"rc={pv.returncode} out={pv.stdout!r}")
            (rt / "claude.progress.json").write_text("garbage {{{",
                                                     encoding="utf-8")
            pv = listener_cli("progress", "claude", rt, db=db, cfg=cfg)
            check("junk progress.json: rc0, honest unknown",
                  pv.returncode == 0
                  and pv.stdout.strip() == "[claude] unknown msg#999",
                  f"rc={pv.returncode} out={pv.stdout!r}")
            bad_cfg = d / "bad-config.json"
            bad_cfg.write_text("{oops", encoding="utf-8")
            pv = listener_cli("progress", "claude", rt, db=db, cfg=bad_cfg)
            check("malformed config: rc0 and fail-closed suffix",
                  pv.returncode == 0
                  and pv.stdout.strip()
                  == "[claude] unknown msg#999 (auto off)",
                  f"rc={pv.returncode} out={pv.stdout!r}")
        finally:
            w.terminate()
            try:
                w.wait(timeout=5)
            except subprocess.TimeoutExpired:
                w.kill()
    finally:
        if p is not None:
            stop_listener("claude", rt, cfg, proc=p)


def test_53_progress_line_speed(d: Path):
    # The spec budget: a statusline repaint costs the user a terminal
    # refresh — one snapshot must render in well under 300 ms including the
    # sender lookup. A live listener whose pid /proc verifies, a trusted
    # progress document (written here in the tracker's exact shape) and a
    # seeded sender keep the whole hot path under measurement.
    db = new_db(d)
    cfg = write_cfg(d)
    rt = d / "runtime"
    rt.mkdir(parents=True, exist_ok=True)
    mid = seed(db)
    w = subprocess.Popen(listener_cmd("watch", "claude", rt, cfg=cfg),
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, env=listener_env())
    try:
        (rt / "claude.listener.pid").write_text(f"{w.pid}\n")
        (rt / "claude.progress.json").write_text(json.dumps({
            "listener_pid": w.pid, "updated_at": time.time(),
            "current": {"message": mid, "stage": "supervising",
                        "supervisor_pid": w.pid + 1, "since": time.time()},
            "last": None}), encoding="utf-8")
        line = listener_cli("progress", "claude", rt, db=db,
                            cfg=cfg).stdout.strip()
        check("sender lookup engaged on the measured path",
              f"msg#{mid} sender=kimi" in line, line)
        worst = 0.0
        for _ in range(5):
            t0 = time.perf_counter()
            pv = listener_cli("progress", "claude", rt, db=db, cfg=cfg)
            dt = time.perf_counter() - t0
            worst = max(worst, dt)
            check("snapshot rc0", pv.returncode == 0)
        check(f"snapshot under 300 ms (worst {worst * 1000:.0f} ms)",
              worst < 0.300, f"worst={worst:.3f}s")
    finally:
        w.terminate()
        try:
            w.wait(timeout=5)
        except subprocess.TimeoutExpired:
            w.kill()


def test_54_dispatcher_log_perms_0600(d: Path):
    # P1-4: the log is created via os.open(..., 0o600) instead of umask, a
    # legacy 0644 file is pressed down on the next write, and rotation must
    # not leave a world-readable dispatcher.log.1 behind. The disabled marker
    # makes run() rotate + write its one start line and exit immediately.
    rt = d / "runtime"
    rt.mkdir(parents=True, exist_ok=True)
    log = rt / "dispatcher.log"
    log.write_bytes(b"x" * 1_000_001)          # over LOG_MAX_BYTES -> rotate
    log.chmod(0o644)                            # legacy state: created by umask
    (rt / "claude.disabled").write_text("test\n", encoding="utf-8")
    r = listener_cli("run", "claude", rt)
    check("run rc0 with disabled marker", r.returncode == 0, r.stderr[-300:])
    check("fresh dispatcher.log exists", log.exists())
    check("fresh dispatcher.log is 0600",
          log.stat().st_mode & 0o777 == 0o600,
          oct(log.stat().st_mode & 0o777))
    rot = rt / "dispatcher.log.1"
    check("rotated log.1 exists", rot.exists())
    check("rotated log.1 is 0600",
          rot.stat().st_mode & 0o777 == 0o600,
          oct(rot.stat().st_mode & 0o777))
    # legacy press-down without rotation: a sub-threshold 0644 log must be
    # chmod'ed back to 0600 by the next write.
    log.chmod(0o644)
    r2 = listener_cli("run", "claude", rt)
    check("second run rc0", r2.returncode == 0, r2.stderr[-300:])
    check("legacy 0644 log pressed to 0600 on write",
          log.stat().st_mode & 0o777 == 0o600,
          oct(log.stat().st_mode & 0o777))


# ------------------------------------------------------------------- main

def main() -> int:
    print(f"tmp: {TMP}")
    tests = [
        test_00_schema_idempotent,
        test_01_auto_success_claude,
        test_02_auto_success_kimi,
        test_03_manual_mode,
        test_04_auto_skips,
        test_05_concurrent_single_execution,
        test_06_crash,
        test_07_silent0,
        test_08_blocked,
        test_09_timeout_kills_group,
        test_10_no_loop_results_ignite_nothing,
        test_11_reconcile_and_recover,
        test_15_payload_safety,
        test_16_rotation,
        test_17_missing_db,
        test_18_launcher_stubs,
        test_12_listener_autostart_survives_mcp_exit,
        test_13_listener_singleton_lock,
        test_14_listener_disable_enable_and_autostart_gates,
        test_19_listener_e2e_claude_no_retry,
        test_20_listener_ignores_non_candidates,
        test_21_two_listeners_one_execution,
        test_22_listener_no_retry_after_crash_or_restart,
        test_30_conversation_send_rules,
        test_31_conversation_limits,
        test_32_blocked_flow_inflight,
        test_33_supervisor_conversation_task,
        test_34_blocked_then_needs_human,
        test_35_hard_failure_freezes_conversation,
        test_41_result_on_result_uncertain,
        test_42_kimi_gate_blocks_review,
        test_39_legacy_schema_fallback,
        test_40_bounded_history,
        test_36_dialogue_e2e_kimi_initiator,
        test_37_dialogue_e2e_claude_initiator,
        test_38_listener_skips_dead_conversations,
        test_43_fresh_db_listener_end_to_end_dispatch,
        test_44_schema_ensure_preserves_existing_rows,
        test_45_progress_stages_sequence,
        test_46_progress_terminal_and_stale_recovery,
        test_47_status_backcompat_without_progress_file,
        test_48_progress_and_status_no_payload,
        test_49_blocked_report_alt_dedup_key,
        test_50_blocked_report_reconcile_recover,
        test_51_progress_line_format_contract,
        test_52_progress_line_history_and_robust,
        test_53_progress_line_speed,
        test_54_dispatcher_log_perms_0600,
    ]
    try:
        for fn in tests:
            run_test(fn)
    finally:
        os.environ.pop("AGENT_BRIDGE_AUTO_LISTEN", None)
        os.environ.pop("FAKE_SUP_LOG", None)
        os.environ.pop("FAKE_SUP_AGENT", None)
        os.environ.pop("FAKE_SUP_DELAY", None)
        os.environ.pop("FAKE_SUP_PROTOCOL", None)
        os.environ["AGENT_ID"] = "claude"
        shutil.rmtree(TMP, ignore_errors=True)
    if FAILURES:
        print(f"\n{len(FAILURES)} FAILURE(S):")
        for f in FAILURES:
            print(" -", f)
        return 1
    print(f"\nALL PASSED ({len(tests)} tests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

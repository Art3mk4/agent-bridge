#!/usr/bin/env python3
"""on_message.py — one-shot supervisor for exactly ONE bridge message.

Started by the per-agent listener systemd/listener.py (flag --auto; also
manually, where --auto is deliberately not required) as a short-lived
process, one per message: validate the candidate, insert the atomic
dispatch_attempts row (the retry barrier), run one headless worker chosen by
message.recipient (cclocal_headless.sh or kimi_headless.sh), wait, classify
the outcome by protocol evidence, notify the original sender on failure and
exit. This process itself holds no daemon, polling loop, systemd unit or
lock — independent supervisors for different messages are fine;
UNIQUE(message_id) plus the worker's own claim-before-work prevent double
execution.

Anti-loop barrier: automatic runs happen only for legacy ROOT tasks (reply_to
IS NULL, no conversation) and for 'task'/'result' messages of a STILL-ACTIVE,
unexpired conversation (the supervisor re-checks the conversation row itself;
a dead or missing one is a Skip). Notices never wake anything in any mode,
and a conversation message never gets a courtesy answer: the prompt gives the
worker/reviewer exactly one protocol with terminal outcomes (result+ack /
complete / followup / needs_human). Success/done evidence is a report under
'<prefix>:<id>:result' (the prescribed key); a blocker filed under
'<prefix>:<id>:blocked' is accepted as honest 'blocked' evidence too, so an
explicit stop never looks like a dispatcher failure. A hard dispatch failure
(failed/timeout/interrupted/uncertain) moves an active conversation to
'blocked' so no further automatic turn can be dispatched into a broken chain.
Whether a
recipient is automated at all is a config decision (config.json "auto");
recipient=kimi is disabled by default because Kimi CLI -p applies its "auto"
permission policy without prompts (see kimi_headless.sh and README).

No automatic retry, ever: a crashed, timed-out or uncertain attempt stays in
the DB with a short payload-free notice; a human decides (manual run of this
script or resetting the attempt row). Manual invocations (without --auto)
bypass the auto gates deliberately — they ARE the human decision. --recover
re-examines only messages without any attempt row and only classifies stale
attempts by PID/result evidence, never reruns uncertain work.

Logs (runtime/dispatcher.log) carry only ids, states and exit codes; worker
stdout goes to 0600 files with keep-newest rotation; the kimi request file
holds the prompt (0600) instead of argv, so no task payload or secrets leak
into logs, argv or the process list.

Usage:
    on_message.py <message_id> [--auto]          # one message (hook/manual)
    on_message.py --recover                      # retry only never-attempted
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent        # agent_bridge/systemd
ROOT = HERE.parent                            # agent_bridge
# Worker cwd: the project both CLIs open (their project MCP configs live
# there). Defaults to the directory that contains this repository; set
# AGENT_BRIDGE_WORKSPACE (e.g. in the MCP server env) when the clone lives
# elsewhere — the listener and its supervisors inherit that environment.
WORKSPACE = Path(os.path.expanduser(
    os.environ.get("AGENT_BRIDGE_WORKSPACE") or str(ROOT.parent))).resolve()

AGENT_PREFIX = {"claude": "cclocal", "kimi": "kimi"}
WORKER_LABEL = {
    "claude": "cclocal (Claude Code, официальный клиент от Anthropic)",
    "kimi": "kimi (Kimi CLI, официальный клиент от Moonshot AI)",
}
PERMISSION_NOTE = {
    "claude": (
        "U тебя нет интерактивного подтверждения: клиент запущен с\n"
        "--permission-prompts none, любое действие, требующее разрешения,\n"
        "отклоняется автоматически — при таком отказе оформляй blocked."
    ),
    "kimi": (
        "Kimi CLI в режиме -p не запрашивает approvals и работает по своей\n"
        "стандартной permission policy (auto). Поэтому строго соблюдай пункт 2\n"
        "протокола: не выполняй деструктивных и внешних действий сверх явно\n"
        "требуемого текстом задачи, не ослабляй permissions; при любом\n"
        "сомнении оформляй blocked вместо действия."
    ),
}

STATE_SPAWNED = "spawned"
STATE_SUCCEEDED = "succeeded"
STATE_BLOCKED = "blocked"
STATE_FAILED = "failed"
STATE_TIMEOUT = "timeout"
STATE_INTERRUPTED = "interrupted"
STATE_UNCERTAIN = "uncertain"
NOTIFY_STATES = {STATE_FAILED, STATE_TIMEOUT, STATE_INTERRUPTED, STATE_UNCERTAIN}

WORKER_TIMEOUT_SECONDS = 3300.0  # 55 min, under the worker's 60-min claim lease
OUTPUT_KEEP = 20
SPAWN_GRACE_SECONDS = 60.0       # 'spawned' without pid younger than this: alive
# A recorded worker pid only counts as "our worker, still running" if /proc
# says that process started within this window around the attempt row —
# a recycled pid belongs to a process started much later.
PID_START_SLACK_SECONDS = 2.0

# Bounded conversation context for the worker prompt: the reviewer needs the
# goal and recent history, but a prompt must never grow with the dialogue.
HISTORY_LIMIT = 12               # messages per side of the current one
HISTORY_ITEM_CHARS = 1200        # per-message truncation
HISTORY_TOTAL_CHARS = 24000      # whole {{CONTEXT}} block cap
GOAL_CHARS = 4000


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("message_id", nargs="?", type=int)
    p.add_argument("--recover", action="store_true",
                   help="process root messages that never got an attempt row "
                        "and reconcile stale attempts; never reruns attempted ones")
    p.add_argument("--auto", action="store_true",
                   help="automatic mode (set by the listener): enforce the "
                        "config gate, root-only/live-conversation and "
                        "message-age checks")
    p.add_argument("--db", default=str(ROOT / "bridge.db"))
    p.add_argument("--config", default=str(HERE / "config.json"))
    p.add_argument("--runtime-dir", default=str(HERE / "runtime"))
    p.add_argument("--prompt-template", default=str(HERE / "dispatcher_prompt.md"))
    p.add_argument("--claude-launcher", default=str(HERE / "cclocal_headless.sh"))
    p.add_argument("--kimi-launcher", default=str(HERE / "kimi_headless.sh"))
    p.add_argument("--workspace", default=str(WORKSPACE),
                   help="worker cwd; project MCP configs live here")
    p.add_argument("--mcp-config", default=str(WORKSPACE / ".mcp.json"),
                   help="claude worker: --strict-mcp-config source")
    p.add_argument("--worker-timeout", type=float, default=WORKER_TIMEOUT_SECONDS)
    args = p.parse_args(argv)
    if args.recover == (args.message_id is not None):
        p.error("exactly one of <message_id> or --recover is required")
    return args


def _connect(db: str) -> sqlite3.Connection:
    # Shared recipe (schema.connect): busy_timeout first, then a WAL switch
    # that is retried by hand — on a not-yet-WAL file the switch fails at
    # once while another writer holds a lock, busy_timeout or not (P0-1).
    return _schema_mod().connect(db)


_SCHEMA_MOD: Any = None


def _schema_mod() -> Any:
    """Load systemd/schema.py, the single owner of the dispatcher DDL. Not a
    plain `import schema`: sibling modules are not importable when this file
    is loaded by path (server.py does exactly that with listener.py), so the
    same spec_from_file_location loader is used here."""
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


def _init_schema(con: sqlite3.Connection) -> None:
    # Purely additive: a new table owned by the dispatcher. The messages
    # schema and the MCP contract are untouched, so this is safe against a
    # live bridge.db (retried on WAL-mode-change BUSY like server._init_db).
    # The full retry budget lives here; the listener calls the same function
    # with a smaller one (schema.py).
    _schema_mod().ensure_dispatch_schema(con)


def _setup_logging(runtime_dir: Path) -> logging.Logger:
    log = logging.getLogger("on_message")
    if not log.handlers:
        log.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s %(levelname)s supervisor: %(message)s")
        # 0600 from creation (P1-4): the supervisor often writes the shared
        # log first, and a plain FileHandler would create it by umask.
        fh = _schema_mod().private_file_handler(runtime_dir / "dispatcher.log")
        fh.setFormatter(fmt)
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        log.addHandler(fh)
        log.addHandler(sh)
        log.propagate = False
    return log


def _load_config(config_path: str, log: logging.Logger) -> dict[str, Any]:
    # config.json is optional: the default automates only claude. A broken
    # config automates nothing (fail closed) and is logged by exception type
    # only — never its content. Same reader as the listener's.
    cfg, err = _schema_mod().load_dispatch_config(config_path)
    if err is not None:
        log.error("unreadable dispatch config (kind=%s), automation disabled",
                  err)
    return cfg


def _auto_enabled(config_path: str, recipient: str, log: logging.Logger) -> bool:
    return bool(_load_config(config_path, log)["auto"].get(recipient, False))


_MSG_COLS = ("id, sender, recipient, text, reply_to, created_at,"
             " response_deadline, acknowledged, claim_expires_at")


def _fetch_message(con: sqlite3.Connection, message_id: int) -> tuple | None:
    # 12 columns: the 9 classic ones plus conversation_id, kind, turn. On a
    # DB that never saw the additive server.py migration the conversation
    # triple reads as (None, None, None) — pure legacy behaviour, no crash.
    try:
        return con.execute(
            f"SELECT {_MSG_COLS}, conversation_id, kind, turn"
            " FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = con.execute(
            f"SELECT {_MSG_COLS} FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        return None if row is None else row + (None, None, None)


def _conv_schema(con: sqlite3.Connection) -> bool:
    # True when the additive conversations layer exists in this DB.
    return _schema_mod().has_conversations(con)


class Skip(Exception):
    """Benign validation reject: log and exit 0, no attempt row."""


def _validate(con: sqlite3.Connection, message_id: int, auto: bool,
              args: argparse.Namespace, log: logging.Logger) -> tuple:
    row = _fetch_message(con, message_id)
    if row is None:
        raise Skip(f"message {message_id} not found")
    (_id, _sender, recipient, _text, reply_to, created, deadline,
     acked, claim_expires, conversation_id, kind, _turn) = row
    if recipient not in AGENT_PREFIX:
        raise Skip(f"message {message_id}: recipient {recipient!r} is not an agent")
    if acked:
        raise Skip(f"message {message_id} already acknowledged")
    if claim_expires is not None and claim_expires > time.time():
        raise Skip(f"message {message_id} is actively claimed, leaving it alone")
    if auto:
        if kind == "notice":
            # Operational notices are never work in any mode.
            raise Skip(f"message {message_id} is a notice")
        if conversation_id is not None or kind is not None:
            # Conversation message: replies are the design, the conversation
            # state machine is the anti-loop barrier instead of reply_to.
            if conversation_id is None or kind not in ("task", "result"):
                raise Skip(f"message {message_id} has a malformed conversation link")
            if not _conv_schema(con):
                raise Skip(f"message {message_id}: no conversations schema")
            conv = con.execute(
                "SELECT state, expires_at FROM conversations WHERE id = ?",
                (conversation_id,),
            ).fetchone()
            if conv is None:
                raise Skip(f"message {message_id}: conversation "
                           f"{conversation_id} row missing")
            if conv[0] != "active":
                raise Skip(f"message {message_id}: conversation "
                           f"{conversation_id} is {conv[0]}")
            if conv[1] <= time.time():
                raise Skip(f"message {message_id}: conversation "
                           f"{conversation_id} is expired")
        elif reply_to is not None:
            # Legacy anti-loop barrier: only root tasks may wake a worker.
            raise Skip(f"message {message_id} is a reply, root tasks only")
        # response_deadline is metadata, not a dispatch gate (P1-8): a
        # message that arrived while the listener was down is still
        # delivered. Only an old backlog waits for a human.
        cfg = _load_config(args.config, log)
        if created is None or created < time.time() - cfg["max_age_seconds"]:
            raise Skip(f"message {message_id} is older than "
                       f"dispatch_max_age_seconds, leaving it to a human")
        if not cfg["auto"].get(recipient, False):
            raise Skip(f"message {message_id}: recipient {recipient} disabled by config")
    else:
        log.info("manual mode for message %s: auto gates bypassed (root=%s,"
                 " deadline_passed=%s)", message_id, reply_to is None,
                 bool(deadline is not None and deadline <= time.time()))
    return row


def _create_attempt(con: sqlite3.Connection, message_id: int) -> bool:
    # Atomic guard BEFORE spawning: exactly one attempt row per message can
    # ever exist; a lost race (hook vs recover, two hooks, a rerun) means no
    # worker is launched.
    try:
        con.execute(
            "INSERT INTO dispatch_attempts (message_id, state, started_at)"
            " VALUES (?, ?, ?)",
            (message_id, STATE_SPAWNED, time.time()),
        )
        con.commit()
        return True
    except sqlite3.IntegrityError:
        return False


_CONV_RULES = (
    "В диалоге разрешены только локальные обратимые действия в рамках цели.\n"
    "Деплой, git push, сообщения наружу, покупки, удаление и любые\n"
    "необратимые или внешние действия — не выполнять: для исполнителя это\n"
    "повод оформить blocked, для ревьюера — needs_human."
)


def _conversation_context(con: sqlite3.Connection, row: tuple) -> str:
    """Bounded {{CONTEXT}}: goal, state, turn budget and the recent messages
    of the conversation (the current one excluded). Empty string for legacy
    messages. Payload appears in prompts only — never in logs, argv, DB."""
    mid, cid, kind = row[0], row[9], row[10]
    if cid is None or kind not in ("task", "result") or not _conv_schema(con):
        return ""
    conv = con.execute(
        "SELECT goal, initiator, state, turn_count, max_turns, expires_at"
        " FROM conversations WHERE id = ?",
        (cid,),
    ).fetchone()
    if conv is None:  # only reachable via a manual run on a corrupt link
        return (f"\nДиалог {cid}: запись в БД отсутствует — повреждённая\n"
                "ссылка; заверши работу со словом \"skipped\".\n")
    goal, initiator, state, turn_count, max_turns, expires_at = conv
    role = ("исполнитель задачи" if kind == "task"
            else "ревьюер результата (ты автор цели — финальное решение за "
                 "тобой)")
    header = (
        f"\n=== Контекст автономного диалога {cid} ===\n"
        f"Цель (поставлена {initiator}): {(goal or '')[:GOAL_CHARS]}\n"
        f"Состояние: {state}; ход {turn_count} из {max_turns}; истекает "
        f"{expires_at:.0f}.\n"
        f"Твоя роль в этом сообщении: {role}.\n{_CONV_RULES}\n"
        "История (старые сверху, без текущего сообщения):\n"
    )
    items = con.execute(
        "SELECT id, sender, kind, text FROM messages"
        " WHERE conversation_id = ? AND id != ?"
        " ORDER BY id DESC LIMIT ?",
        (cid, mid, HISTORY_LIMIT),
    ).fetchall()
    lines: list[str] = []  # kept newest-first up to the cap, printed oldest-first
    total = len(header)
    for hid, hsender, hkind, htext in items:  # newest first
        body = htext or ""
        if len(body) > HISTORY_ITEM_CHARS:
            body = body[:HISTORY_ITEM_CHARS] + "…"
        line = f"[id {hid}] {hkind or 'msg'} {hsender}: {body}\n"
        if total + len(line) > HISTORY_TOTAL_CHARS:
            break  # drop the oldest, never the newest
        lines.insert(0, line)
        total += len(line)
    if not lines:
        lines.append("(пусто — это первое сообщение диалога)\n")
    return header + "".join(lines) + "=== конец контекста ===\n"


def _protocol_text(recipient: str, sender: str, conversation_id: int | None,
                   kind: str | None, message_id: int) -> str:
    """Exactly one deterministic protocol per message role — a legacy root
    task, a conversation task (do the work, answer once) or a conversation
    result review (exactly one terminal outcome, no courtesy replies)."""
    prefix = AGENT_PREFIX[recipient]
    mid, peer = str(message_id), sender
    common_claim = (
        f"1. ДО любой работы вызови mcp__agent_bridge__claim_message("
        f"message_id={mid}, lease_seconds=3600). Если claimed=false "
        "(claimed_by_other / acknowledged / not_found) — ничего не "
        "выполняй, завершись со словом \"skipped\".\n"
    )
    if conversation_id is not None and kind == "result":
        cid = str(conversation_id)
        return common_claim + (
            "2. Сравни результат (текст выше) с целью и историей диалога.\n"
            "   Выбери РОВНО ОДИН итог:\n"
            "   a) цель достигнута: mcp__agent_bridge__conversation("
            f'action="complete", conversation_id={cid}), затем\n'
            f"   mcp__agent_bridge__mark_read([{mid}]). НИЧЕГО не "
            "отправляй —\n   вежливый ответ запрещён, диалог закрыт.\n"
            "   b) есть конкретный следующий шаг: \n"
            "   mcp__agent_bridge__send_message(\n"
            f'   recipient="{peer}", reply_to={mid},\n'
            f"   conversation_id={cid}, kind=\"task\",\n"
            f'   dedup_key="{prefix}:{mid}:followup",\n'
            "   text=конкретная следующая задача), затем\n"
            f"   mcp__agent_bridge__mark_read([{mid}]).\n"
            "   c) нужен человек: неуверенный или противоречащий цели\n"
            "   результат, требуемый внешний/необратимый шаг, ошибка\n"
            "   любого вызова выше: mcp__agent_bridge__conversation(\n"
            f'   action="needs_human", conversation_id={cid}), затем\n'
            f"   mcp__agent_bridge__mark_read([{mid}]), сообщений не шлёт.\n"
            "3. Запрещено: отвечать kind=\"result\" на результат; дублировать\n"
            "   уже поставленные задачи; деплой, git push, внешние и\n"
            "   необратимые действия (это всегда итог c); повторять чужую\n"
            "   работу. Автоматических повторов нет — это единственный\n"
            "   обзор данного результата.\n"
        )
    if conversation_id is not None and kind == "task":
        cid = str(conversation_id)
        return common_claim + (
            "2. Выполни задачу в рамках цели диалога (контекст выше).\n"
            f"   {_CONV_RULES}\n"
            "3. Успех: mcp__agent_bridge__send_message(\n"
            f'   recipient="{peer}", reply_to={mid},\n'
            f"   conversation_id={cid}, kind=\"result\",\n"
            f'   dedup_key="{prefix}:{mid}:result",\n'
            "   text=отчёт: что сделано, как проверено, что не удалось),\n"
            f"   затем mcp__agent_bridge__mark_read([{mid}]).\n"
            "4. Блокер или провал (отказ permission, нужен внешний или\n"
            "   необратимый шаг): тот же вызов, но\n"
            '   text="blocked: <конкретная причина>", затем\n'
            f"   mcp__agent_bridge__release_message({mid}). НЕ вызывай\n"
            "   mark_read. Ревьюер увидит blocked и оформит needs_human.\n"
            "5. Автоматических повторов нет: это единственная попытка,\n"
            "   ответ на результат порождает только ревьюер диалога.\n"
        )
    return common_claim + (
        "2. Выполни задачу из текста. Соблюдай правила проекта (CLAUDE.md) и\n"
        "   общие ограничения: не запускай agent_bridge/test_bridge.py на\n"
        "   live bridge.db, не ослабляй permissions, не выполняй\n"
        "   деструктивных и внешних действий (удаление файлов вне задачи,\n"
        "   git push, отправка данных наружу), если их явным образом не\n"
        "   требует текст сообщения.\n"
        "3. Успех: отправь mcp__agent_bridge__send_message(\n"
        f'   recipient="{peer}", reply_to={mid},\n'
        f'   dedup_key="{prefix}:{mid}:result",\n'
        "   text=итоговый отчёт: что сделано, как проверено, что не\n"
        f"   удалось), затем mcp__agent_bridge__mark_read([{mid}]).\n"
        "4. Блокер или провал (в том числе из-за отказа permission): \n"
        "   отправь mcp__agent_bridge__send_message(\n"
        f'   recipient="{peer}", reply_to={mid},\n'
        f'   dedup_key="{prefix}:{mid}:result",\n'
        '   text="blocked: <конкретная причина>"), затем\n'
        f"   mcp__agent_bridge__release_message({mid}). НЕ вызывай mark_read.\n"
        "5. Автоматически запускаются только корневые задачи (reply_to IS\n"
        "   NULL): твой результат — обычный unread в inbox отправителя,\n"
        "   новое воркер-сообщение он не порождает. Никаких автоматических\n"
        "   повторов: это единственная попытка, ошибка фиксируется\n"
        "   человеком.\n"
    )


def _build_prompt(template: str, row: tuple,
                  con: sqlite3.Connection) -> str:
    out = template
    recipient = row[2]
    deadline = row[6]
    for token, value in (
        ("{{MESSAGE_ID}}", str(row[0])),
        ("{{SENDER}}", row[1]),
        ("{{REPLY_TO}}", "none" if row[4] is None else str(row[4])),
        ("{{CREATED_AT}}", f"{row[5]:.3f}"),
        ("{{RESPONSE_DEADLINE}}", "none" if deadline is None
         else f"{deadline:.3f}"),
        ("{{TEXT}}", row[3]),
        ("{{RESULT_PREFIX}}", AGENT_PREFIX[recipient]),
        ("{{WORKER_LABEL}}", WORKER_LABEL[recipient]),
        ("{{PERMISSION_NOTE}}", PERMISSION_NOTE[recipient]),
        ("{{CONTEXT}}", _conversation_context(con, row)),
        ("{{PROTOCOL}}", _protocol_text(recipient, row[1], row[9], row[10],
                                        row[0])),
    ):
        out = out.replace(token, value)
    return out


def _rotate_outputs(runtime_dir: Path) -> None:
    for pattern in ("worker_*.out", "request_*.txt"):
        files = sorted(
            runtime_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True
        )
        for old in files[OUTPUT_KEEP:]:
            try:
                old.unlink()
            except OSError:
                pass


def _worker_command(args: argparse.Namespace, con: sqlite3.Connection,
                    row: tuple, runtime_dir: Path):
    """Per-recipient worker invocation (symmetry). Returns
    (argv, prompt_stdin_bytes_or_None, extra_env). The payload goes to the
    claude worker via stdin and to the kimi worker via a 0600 request file
    referenced by one short prompt line — never into argv/process list."""
    message_id = row[0]
    recipient = row[2]
    prompt = _build_prompt(
        Path(args.prompt_template).read_text(encoding="utf-8"), row, con
    )
    if recipient == "claude":
        argv = [
            args.claude_launcher,
            "--print",
            "--output-format",
            "json",
            "--no-session-persistence",
            "--permission-prompts",
            "none",
            "--mcp-config",
            args.mcp_config,
            "--strict-mcp-config",
        ]
        return argv, prompt.encode("utf-8"), {}
    # kimi: request file keeps the task text out of argv entirely.
    request_path = runtime_dir / f"request_{message_id}.txt"
    fd = os.open(request_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(prompt.encode("utf-8"))
    argv = [
        args.kimi_launcher,
        "--prompt",
        f"Прочитай файл {request_path} и строго выполни описанный в нём "
        "протокол для одного сообщения. Не выводи его содержимое в вывод.",
        "--output-format",
        "stream-json",
    ]
    return argv, None, {"KIMI_CODE_TRUST_WORKSPACE": "1"}


def _run_worker(args: argparse.Namespace, con: sqlite3.Connection, row: tuple,
                runtime_dir: Path, log: logging.Logger) -> tuple[int | None, bool]:
    message_id, recipient = row[0], row[2]
    argv, prompt_stdin, extra_env = _worker_command(args, con, row, runtime_dir)
    out_path = runtime_dir / f"worker_{message_id}.out"
    out_fd = os.open(out_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    env = os.environ.copy()  # each launcher rebuilds its own client env
    env.update(extra_env)
    try:
        proc = subprocess.Popen(  # no shell=True, direct argv
            argv,
            stdin=(subprocess.PIPE if prompt_stdin is not None
                   else subprocess.DEVNULL),
            stdout=out_fd,
            stderr=subprocess.STDOUT,
            cwd=args.workspace,
            start_new_session=True,  # own group, killed as a whole on timeout
            env=env,
        )
    finally:
        os.close(out_fd)
    _rotate_outputs(runtime_dir)  # after the current file exists: dir keeps ≤20
    con.execute(
        "UPDATE dispatch_attempts SET pid = ? WHERE message_id = ?",
        (proc.pid, message_id),
    )
    con.commit()
    log.info("worker spawned message_id=%s recipient=%s pid=%s",
             message_id, recipient, proc.pid)
    timed_out = False
    try:
        proc.communicate(input=prompt_stdin, timeout=args.worker_timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
    return proc.returncode, timed_out


def _report_evidence(con: sqlite3.Connection, message_id: int,
                     recipient: str) -> str | None:
    """Which report key proves this worker finished reporting: the prescribed
    '<prefix>:<id>:result', or a self-reported blocker under
    '<prefix>:<id>:blocked'.

    The protocol prescribes ':result' (with text starting "blocked: ..."), and
    that stays the documented form. A worker that files the blocker under its
    own dedup suffix has still left provable, side-effect-free evidence that
    the protocol reached its end — classifying that as 'uncertain' punished an
    honest report with a frozen dialogue and a false dispatch-error notice.
    ':result' wins if both keys exist."""
    prefix = AGENT_PREFIX[recipient]
    result_key = f"{prefix}:{message_id}:result"
    blocked_key = f"{prefix}:{message_id}:blocked"
    keys = {r[0] for r in con.execute(
        "SELECT dedup_key FROM messages"
        " WHERE sender = ? AND reply_to = ? AND dedup_key IN (?, ?)",
        (recipient, message_id, result_key, blocked_key),
    ).fetchall()}
    if result_key in keys:
        return "result"
    if blocked_key in keys:
        return "blocked"
    return None


def _has_followup(con: sqlite3.Connection, message_id: int,
                  recipient: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM messages"
        " WHERE sender = ? AND reply_to = ? AND dedup_key = ? LIMIT 1",
        (recipient, message_id,
         f"{AGENT_PREFIX[recipient]}:{message_id}:followup"),
    ).fetchone()
    return row is not None


def _conversation_terminal(con: sqlite3.Connection, conversation_id: int) -> bool:
    # complete/needs_human were set by this reviewer via the conversation
    # tool: a review can legitimately end without sending any message.
    row = con.execute(
        "SELECT 1 FROM conversations WHERE id = ?"
        " AND state IN ('completed', 'needs_human')",
        (conversation_id,),
    ).fetchone()
    return row is not None


def _evidence_done(con: sqlite3.Connection, message_id: int, recipient: str,
                   conversation_id: int | None,
                   kind: str | None) -> tuple[bool, str | None]:
    """Protocol evidence for THIS message's role, plus which report proved it
    ('result' | 'blocked' | 'followup' | 'terminal' | None). A review of a
    conversation 'result' completes without a courtesy message: success is a
    followup task or a terminal conversation state. Everything else keeps the
    classic report-with-dedup evidence, so legacy behavior is byte-for-byte
    intact for ':result' reports. Shared by _classify and _reconcile so that
    --recover labels a crash exactly like a live run would."""
    if conversation_id is not None and kind == "result":
        if _has_followup(con, message_id, recipient):
            return True, "followup"
        if _conv_schema(con) and _conversation_terminal(con, conversation_id):
            return True, "terminal"
        return False, None
    evidence = _report_evidence(con, message_id, recipient)
    return evidence is not None, evidence


def _is_acknowledged(con: sqlite3.Connection, message_id: int) -> bool:
    row = con.execute(
        "SELECT acknowledged FROM messages WHERE id = ?", (message_id,)
    ).fetchone()
    return row is not None and row[0] == 1


def _classify(con: sqlite3.Connection, message_id: int, recipient: str,
              exit_code: int | None, timed_out: bool) -> str:
    # Success means the protocol completed, not merely exit 0: the original
    # message acknowledged AND the role-appropriate terminal evidence exists
    # (result message; for a review: followup or terminal conversation).
    acked = _is_acknowledged(con, message_id)
    row = _fetch_message(con, message_id)
    done, evidence = _evidence_done(con, message_id, recipient,
                                    row[9] if row else None,
                                    row[10] if row else None)
    if timed_out:
        return STATE_TIMEOUT
    if evidence == "blocked":
        # A self-filed blocker under its own dedup key is an honest stop:
        # blocked whatever the exit code says, and blocked is NOT a dispatch
        # error — no notice, no frozen dialogue, no non-zero supervisor rc.
        return STATE_BLOCKED
    if acked and done and exit_code == 0:
        return STATE_SUCCEEDED
    if done and not acked:
        return STATE_BLOCKED  # worker self-reported a status and left unacked
    if exit_code not in (0, None):
        return STATE_FAILED
    return STATE_UNCERTAIN


def _finish_attempt(con: sqlite3.Connection, message_id: int, state: str,
                    exit_code: int | None, error_kind: str | None = None) -> None:
    con.execute(
        "UPDATE dispatch_attempts SET state = ?, finished_at = ?, exit_code = ?,"
        " error_kind = ? WHERE message_id = ?",
        (state, time.time(), exit_code, error_kind, message_id),
    )
    con.commit()


def _mark_conversation_blocked(con: sqlite3.Connection,
                               conversation_id: int | None,
                               log: logging.Logger) -> None:
    """A hard dispatch failure strands its message; freeze the dialogue so no
    further automatic turn is dispatched into a broken chain. Guarded: a
    conversation that already left 'active' keeps its terminal state."""
    if conversation_id is None or not _conv_schema(con):
        return
    now = time.time()
    try:
        cur = con.execute(
            "UPDATE conversations SET state = 'blocked', updated_at = ?"
            " WHERE id = ? AND state = 'active'",
            (now, conversation_id),
        )
        if cur.rowcount == 1:
            con.execute(
                "INSERT INTO conversation_transitions"
                " (conversation_id, at, state, reason)"
                " VALUES (?, ?, 'blocked', 'dispatch_failed')",
                (conversation_id, now),
            )
        con.commit()
        if cur.rowcount == 1:
            log.info("conversation %s moved to blocked (dispatch_failed)",
                     conversation_id)
    except sqlite3.OperationalError:
        con.rollback()


def _notify_sender(con: sqlite3.Connection, row: tuple, state: str,
                   log: logging.Logger) -> None:
    # Short operational notice from the recipient agent to the original
    # sender: reply_to the task, own dedup prefix, no task text, and the
    # original is NOT acknowledged on the worker's behalf. kind='notice'
    # (conversation DBs only) so even an active conversation's listener
    # refuses to wake a worker on it.
    message_id, sender, recipient = row[0], row[1], row[2]
    prefix = AGENT_PREFIX[recipient]
    text = (
        f"dispatcher ({prefix}): автоматическая попытка для сообщения "
        f"{message_id} завершилась в состоянии '{state}'. Исходное сообщение "
        "не подтверждено, автоматический повтор не выполняется — нужно "
        "ручное решение (выполнить вручную или сбросить строку в "
        "dispatch_attempts)."
    )
    now = time.time()
    try:
        if _conv_schema(con):
            con.execute(
                "INSERT INTO messages"
                " (sender, recipient, text, reply_to, created_at,"
                "  response_deadline, dedup_key, conversation_id, kind)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'notice')",
                (recipient, sender, text, message_id, now, now + 3600,
                 f"{prefix}:{message_id}:dispatch-error", row[9]),
            )
        else:
            con.execute(
                "INSERT INTO messages"
                " (sender, recipient, text, reply_to, created_at,"
                "  response_deadline, dedup_key)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (recipient, sender, text, message_id, now, now + 3600,
                 f"{prefix}:{message_id}:dispatch-error"),
            )
        con.commit()
        log.info("dispatch error notice sent message_id=%s state=%s", message_id, state)
    except sqlite3.IntegrityError:
        pass  # notice for this message already delivered (dedup)


def _proc_start(pid: int) -> tuple[str, float] | None:
    """(state letter, start time as epoch seconds) from /proc, or None when
    /proc cannot say (no procfs, process gone, unparsable)."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        # comm may contain spaces and parentheses: split after the LAST ')'.
        fields = stat[stat.rindex(")") + 2:].split()
        state, start_ticks = fields[0], int(fields[19])
        btime = next(int(line.split()[1]) for line in
                     Path("/proc/stat").read_text().splitlines()
                     if line.startswith("btime "))
        return state, btime + start_ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError, StopIteration):
        return None


def _pid_alive(pid: int | None, started_at: float | None = None) -> bool:
    """Is the recorded worker still running?

    os.kill(pid, 0) alone cannot tell our worker from an unrelated process
    that later got the same pid, which would keep a dead attempt in
    'spawned' forever. Where /proc is available the process must also not be
    a zombie and must have started inside the window between the attempt row
    and the spawn grace — a recycled pid starts much later than that."""
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    info = _proc_start(pid)
    if info is None:
        return True  # no /proc evidence either way: keep the old behaviour
    state, start = info
    if state in ("Z", "X"):
        return False
    if started_at is None:
        return True
    return (started_at - PID_START_SLACK_SECONDS <= start
            <= started_at + SPAWN_GRACE_SECONDS)


def _reconcile(con: sqlite3.Connection, log: logging.Logger) -> None:
    # Crash/restart reconciliation, safe to run from several supervisors
    # concurrently (classification is evidence-based, notices are deduped).
    # An attempt left in 'spawned' whose worker pid is gone can never be
    # re-run automatically: classify by the protocol evidence in messages.
    # 'interrupted' = no trace of side effects; 'uncertain' = evidence exists
    # but success cannot be proven. A missing pid within the spawn grace is
    # treated as alive: another supervisor may be between INSERT and spawn.
    for (message_id, pid, started_at) in con.execute(
        "SELECT message_id, pid, started_at FROM dispatch_attempts"
        " WHERE state = ?", (STATE_SPAWNED,)
    ).fetchall():
        if _pid_alive(pid, started_at):
            continue  # worker still running (possibly a sibling supervisor's)
        if pid is None and time.time() - started_at < SPAWN_GRACE_SECONDS:
            continue
        acked = _is_acknowledged(con, message_id)
        row = _fetch_message(con, message_id)
        recipient = row[2] if row else "claude"
        done, evidence = _evidence_done(con, message_id, recipient,
                                        row[9] if row else None,
                                        row[10] if row else None)
        if evidence == "blocked":
            # Same short-circuit as _classify: a self-reported blocker is a
            # provable stop, not a crash, so --recover must not relabel it
            # 'interrupted' just because the supervisor died on the way out.
            state = STATE_BLOCKED
        elif acked and done:
            state = STATE_SUCCEEDED
        elif done:
            state = STATE_BLOCKED
        else:
            state = STATE_INTERRUPTED if not acked else STATE_UNCERTAIN
        _finish_attempt(con, message_id, state, None, "restarted")
        if state in (STATE_INTERRUPTED, STATE_UNCERTAIN):
            # No provable side effects either way: freeze the dialogue.
            _mark_conversation_blocked(con, row[9] if row else None, log)
        log.info("reconciled stale attempt message_id=%s -> %s", message_id, state)


def _process_message(con: sqlite3.Connection, message_id: int,
                     args: argparse.Namespace, log: logging.Logger) -> int:
    """Full lifecycle for one message; returns a process exit code."""
    runtime_dir = Path(args.runtime_dir)
    try:
        row = _validate(con, message_id, args.auto, args, log)
    except Skip as exc:
        log.info("skip: %s", exc)
        return 0
    if not _create_attempt(con, message_id):
        log.info("attempt already exists, skipping message_id=%s", message_id)
        return 0
    log.info("dispatching message_id=%s recipient=%s", message_id, row[2])
    try:
        exit_code, timed_out = _run_worker(args, con, row, runtime_dir, log)
    except OSError as exc:
        # The worker never started (missing/non-executable launcher, bad
        # workspace, unreadable prompt template). Without this the attempt
        # stayed 'spawned' until a later reconcile relabelled it
        # 'interrupted' — and the sender was never told.
        kind = type(exc).__name__
        log.error("worker spawn failed message_id=%s kind=%s", message_id, kind)
        _finish_attempt(con, message_id, STATE_FAILED, None, f"spawn:{kind}")
        _mark_conversation_blocked(con, row[9], log)
        _notify_sender(con, row, STATE_FAILED, log)
        return 3
    state = _classify(con, message_id, row[2], exit_code, timed_out)
    error_kind = None if state == STATE_SUCCEEDED else f"exit={exit_code}"
    _finish_attempt(con, message_id, state, exit_code, error_kind)
    log.info("worker finished message_id=%s state=%s exit=%s timed_out=%s",
             message_id, state, exit_code, timed_out)
    if state in NOTIFY_STATES:
        # Freeze the dialogue first (if any): a broken chain must not keep
        # accepting automatic turns while the human decides what to do.
        _mark_conversation_blocked(con, row[9], log)
        _notify_sender(con, row, state, log)
        return 3
    return 0


def _recover(con: sqlite3.Connection, args: argparse.Namespace,
             log: logging.Logger) -> int:
    # Only provably-never-started work: messages WITHOUT any attempt row.
    # Stale attempts are only classified by _reconcile above, never rerun.
    # Same dispatch set as the listener (schema.find_candidates), including
    # the age bound and excluding any response_deadline gate.
    cfg = _load_config(args.config, log)
    rows = _schema_mod().find_candidates(con, AGENT_PREFIX, time.time(),
                                         cfg["max_age_seconds"])
    rc = 0
    for message_id, recipient, _created in rows:
        if not cfg["auto"].get(recipient, False):
            log.info("recover: message %s recipient disabled by config", message_id)
            continue
        rc = max(rc, _process_message(con, message_id, args, log))
    log.info("recover finished: %d candidate(s)", len(rows))
    return rc


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runtime_dir = Path(args.runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(runtime_dir, 0o700)
    log = _setup_logging(runtime_dir)

    if not Path(args.db).exists():
        log.error("bridge.db not found at %s", args.db)
        return 2
    con = None
    try:
        con = _connect(args.db)
        _init_schema(con)
        _reconcile(con, log)
        if args.recover:
            return _recover(con, args, log)
        return _process_message(con, args.message_id, args, log)
    except Exception:
        log.exception("supervisor failed (message_id=%s)", args.message_id)
        return 4
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)

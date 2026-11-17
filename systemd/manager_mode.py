#!/usr/bin/env python3
"""Manager mode for the interactive Claude Code session (identity `claude`).

In manager mode the main model plans, delegates to the local worker over the
bridge, waits for its signal (`listener.py wait`) and reviews — it does not
write code. The mode is per project and can be switched on in a RUNNING
session, without a restart:

* hooks are installed once into the user's Claude Code settings
  (`manager_mode.py install`); Claude Code snapshots hooks at startup, so
  they start working from the next session start — once, ever;
* every hook call reads the per-project flag in systemd/runtime/manager/, so
  turning the mode on or off takes effect on the very next tool call;
* the MCP tool `manager(action="on")` (or the CLI) turns it on and returns
  the role text, which lands in the model's context at once;
* only a human turns it off (`manager_mode.py off`) — the MCP tool has no
  "off", or the model could lift its own guard.

Hooks (one entry point, `manager_mode.py hook`, dispatched by event name):

* PreToolUse  — blocks Edit/Write/MultiEdit/NotebookEdit and obvious file
  writes through Bash INSIDE a manager-mode project. Allowed there:
  <project>/.agent-tasks/ (task specs) and <project>/.git/info/exclude;
  anything outside the project root (/tmp, /dev/null, ...) is not this
  guard's business. Exit 2 hands the reason to the model.
* UserPromptSubmit — a short reminder of the role on every user turn, so
  the role does not fade over a long session.
* SessionStart — the full role text after startup, /clear and compaction.

Bridge workers are never affected: launchers export AGENT_BRIDGE_WORKER=1,
and interactive OpenClaude shifts run with CLAUDE_CODE_USE_OPENAI=1. Outside
a manager-mode project every hook is a fast no-op (no state file to read).

A guard, not a sandbox: anything the hook cannot parse is let through
(fail-open) and the role itself does the main work. Stdlib only, no bridge
database access, never any message payload.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
BRIDGE_DIR = HERE.parent
# AGENT_BRIDGE_MANAGER_DIR: tests and unusual layouts; the hooks read the
# same variable, so set it for Claude Code too if you change it.
STATE_DIR = Path(os.environ.get("AGENT_BRIDGE_MANAGER_DIR")
                 or HERE / "runtime" / "manager")
ROLE_FILE = BRIDGE_DIR / "commands" / "manager-role.md"
HOOK_EVENTS = ("PreToolUse", "UserPromptSubmit", "SessionStart")
GUARD_MATCHER = "Edit|Write|MultiEdit|NotebookEdit|Bash"
EDIT_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
ALLOWED_DIR = ".agent-tasks"
HOOK_MARK = "manager_mode.py\" hook"

BLOCK_REASON = (
    "Заблокировано режимом Manager: главная модель не меняет файлы проекта "
    "({what}). Поставь задачу исполнителю `local` через "
    "mcp__agent_bridge__start_conversation и дождись сигнала "
    "`listener.py wait` в фоне. Исключение — файлы постановок в "
    "<корень>/.agent-tasks/. Если исполнитель не справляется, опиши правку "
    "человеку."
)
REMINDER = (
    "[agent_bridge: режим Manager для {project}] Код не пишешь: план, "
    "постановка задачи `local` (start_conversation, абсолютные пути), "
    "ожидание фоном без опроса — `python3 \"{bridge}/systemd/listener.py\" "
    "wait --agent claude --conversation <id> --timeout 3600` — затем ревью "
    "(git diff --stat, тесты) и одно решение: complete / доработка / "
    "needs_human."
)


# -------------------------------------------------------------------- state

def project_root(path: str | Path | None) -> Path:
    """The git work-tree root containing `path` (or the path itself when it
    is not inside a repository). The mode is keyed by this root, so it is
    the same from any subdirectory of the project."""
    p = Path(os.path.expanduser(str(path or os.getcwd()))).resolve()
    for cand in (p, *p.parents):
        if (cand / ".git").exists():
            return cand
    return p


def _state_path(root: Path) -> Path:
    digest = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:16]
    return STATE_DIR / f"{digest}.json"


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
        os.chmod(path.parent, 0o700)
    except OSError:
        pass


def state(path: str | Path | None) -> dict[str, Any] | None:
    """The enable record for the project containing `path`, or None."""
    root = project_root(path)
    try:
        doc = json.loads(_state_path(root).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(doc, dict) or doc.get("project") != str(root):
        return None
    return doc


def enable(path: str | Path | None, by: str) -> dict[str, Any]:
    root = project_root(path)
    _private_dir(STATE_DIR)
    doc = {"project": str(root), "enabled_at": time.time(), "by": by}
    target = _state_path(root)
    tmp = target.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False)
    os.replace(tmp, target)
    return doc


def disable(path: str | Path | None) -> bool:
    try:
        _state_path(project_root(path)).unlink()
        return True
    except FileNotFoundError:
        return False


def list_enabled() -> list[dict[str, Any]]:
    out = []
    for f in sorted(STATE_DIR.glob("*.json")) if STATE_DIR.is_dir() else []:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(doc, dict) and doc.get("project"):
            out.append(doc)
    return out


def role_text(root: str | Path) -> str:
    text = ROLE_FILE.read_text(encoding="utf-8")
    return (text.replace("{AGENT_BRIDGE_DIR}", str(BRIDGE_DIR))
                .replace("{PROJECT}", str(root)))


# -------------------------------------------------------------------- guard

def _allowed_path(path: str, cwd: str, root: str) -> bool:
    """May the manager write this path? Everything outside the project root
    is allowed; inside it only task specs and the local git ignore file."""
    if not path:
        return False
    expanded = os.path.expandvars(os.path.expanduser(path))
    full = os.path.normpath(os.path.join(cwd or ".", expanded))
    root = os.path.normpath(root)
    if full != root and not full.startswith(root + os.sep):
        return True
    rel = os.path.relpath(full, root)
    if rel.endswith(os.path.join(".git", "info", "exclude")):
        return True  # the local ignore entry for .agent-tasks/ itself
    return rel.split(os.sep)[0] == ALLOWED_DIR


_HEREDOC = re.compile(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")


def _strip_heredocs(cmd: str) -> str:
    """Drop heredoc bodies (commit messages and the like are not
    redirections), keep the command lines themselves."""
    out, lines, i = [], cmd.split("\n"), 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        words = [m.group(2) for m in _HEREDOC.finditer(line)]
        i += 1
        for word in words:
            while i < len(lines) and lines[i].strip() != word:
                i += 1
            i += 1
    return "\n".join(out)


def _redirect_targets(cmd: str) -> list[str]:
    """Targets of output redirections outside quotes. `2>&1`-style fd
    duplication is not a file; `->` and `=>` are not redirections."""
    targets, i, n, quote = [], 0, len(cmd), None
    while i < n:
        c = cmd[i]
        if quote:
            if c == "\\" and quote == '"':
                i += 2
                continue
            if c == quote:
                quote = None
            i += 1
            continue
        if c == "\\":
            i += 2
            continue
        if c in "'\"":
            quote = c
            i += 1
            continue
        if c == ">" and not (i > 0 and cmd[i - 1] in "-="):
            j = i + 1
            if j < n and cmd[j] in ">|":
                j += 1
            if j < n and cmd[j] == "&":
                i = j + 1
                continue
            while j < n and cmd[j] in " \t":
                j += 1
            k = j
            while k < n and cmd[k] not in " \t\n;|&<>()":
                k += 1
            word = cmd[j:k].strip("'\"")
            if word:
                targets.append(word)
            i = k
            continue
        i += 1
    return targets


_WRITE_CMDS = [
    (re.compile(r"(^|[\s;&|(])sed\s+(-[a-zA-Z]*\s+)*-[a-zA-Z]*i"), "sed -i"),
    (re.compile(r"(^|[\s;&|(])perl\s+(-[a-zA-Z]*\s+)*-[a-zA-Z]*i"), "perl -i"),
    (re.compile(r"(^|[\s;&|(])patch(\s|$)"), "patch"),
    (re.compile(r"(^|[\s;&|(])git\s+(apply|am)(\s|$)"), "git apply/am"),
]
_TEE = re.compile(r"(^|[\s;&|(])tee\s+((?:-[a-z]+\s+)*)([^\s;&|]+)")


def check_bash(cmd: str, cwd: str, root: str) -> str | None:
    """What makes this command a file write inside the project, or None.
    sed -i / perl -i / patch / git apply|am are refused outright: their
    targets are not reliably visible in the command line."""
    body = _strip_heredocs(cmd)
    for target in _redirect_targets(body):
        if not _allowed_path(target, cwd, root):
            return f"перенаправление в {target}"
    for m in _TEE.finditer(body):
        target = m.group(3).strip("'\"")
        if not _allowed_path(target, cwd, root):
            return f"tee в {target}"
    for rx, what in _WRITE_CMDS:
        if rx.search(body):
            return what
    return None


def _is_worker() -> bool:
    """Bridge workers must keep writing code in a manager-mode project:
    headless launchers export AGENT_BRIDGE_WORKER=1, interactive OpenClaude
    shifts run with CLAUDE_CODE_USE_OPENAI=1."""
    return (os.environ.get("AGENT_BRIDGE_WORKER") == "1"
            or os.environ.get("CLAUDE_CODE_USE_OPENAI") == "1")


def handle_hook(event: dict[str, Any]) -> tuple[int, str, str]:
    """(exit code, stdout, stderr) for one hook event."""
    if _is_worker():
        return 0, "", ""
    cwd = str(event.get("cwd") or os.getcwd())
    doc = state(cwd)
    if doc is None:
        return 0, "", ""
    root = doc["project"]
    name = event.get("hook_event_name") or ""
    if name == "UserPromptSubmit":
        return 0, REMINDER.format(project=root, bridge=BRIDGE_DIR), ""
    if name == "SessionStart":
        try:
            return 0, role_text(root), ""
        except OSError:
            return 0, REMINDER.format(project=root, bridge=BRIDGE_DIR), ""
    if name != "PreToolUse":
        return 0, "", ""
    tool = event.get("tool_name") or ""
    args = event.get("tool_input") or {}
    what = None
    if tool in EDIT_TOOLS:
        path = str(args.get("file_path") or args.get("notebook_path") or "")
        if not _allowed_path(path, cwd, root):
            what = f"{tool} {path}"
    elif tool == "Bash":
        what = check_bash(str(args.get("command") or ""), cwd, root)
    if what is None:
        return 0, "", ""
    return 2, "", BLOCK_REASON.format(what=what)


# ------------------------------------------------------------------ install

def settings_path(explicit: str | None = None) -> Path:
    if explicit:
        return Path(os.path.expanduser(explicit))
    base = os.environ.get("CLAUDE_CONFIG_DIR") or "~/.claude"
    return Path(os.path.expanduser(base)) / "settings.json"


def hook_command() -> str:
    return f'python3 "{HERE / "manager_mode.py"}" hook'


def _our(entry: Any) -> bool:
    return isinstance(entry, dict) and any(
        isinstance(h, dict) and HOOK_MARK in str(h.get("command", ""))
        for h in entry.get("hooks", []) or [])


def hooks_installed(path: Path) -> bool:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    hooks = doc.get("hooks") if isinstance(doc, dict) else None
    if not isinstance(hooks, dict):
        return False
    return all(any(_our(e) for e in hooks.get(ev, []) or [])
               for ev in HOOK_EVENTS)


def install(path: Path, remove: bool = False) -> dict[str, Any]:
    """Merge (or remove) our hooks in a Claude Code settings file. Other
    keys and other hooks are kept; the previous file is backed up next to
    it. Idempotent."""
    try:
        raw = path.read_text(encoding="utf-8")
        doc = json.loads(raw) if raw.strip() else {}
    except FileNotFoundError:
        raw, doc = None, {}
    if not isinstance(doc, dict):
        raise ValueError(f"{path} is not a JSON object")
    hooks = doc.setdefault("hooks", {})
    if not isinstance(hooks, dict):
        raise ValueError(f"{path}: 'hooks' is not an object")
    cmd = hook_command()
    for ev in HOOK_EVENTS:
        kept = [e for e in hooks.get(ev, []) or [] if not _our(e)]
        if not remove:
            entry: dict[str, Any] = {"hooks": [{"type": "command",
                                                "command": cmd}]}
            if ev == "PreToolUse":
                entry = {"matcher": GUARD_MATCHER, **entry}
            kept.append(entry)
        if kept:
            hooks[ev] = kept
        else:
            hooks.pop(ev, None)
    if not hooks:
        doc.pop("hooks", None)
    new = json.dumps(doc, ensure_ascii=False, indent=2) + "\n"
    backup = None
    if raw is not None and raw != new:
        backup = path.with_name(f"{path.name}.bak-{int(time.time())}")
        backup.write_text(raw, encoding="utf-8")
    if raw != new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(new, encoding="utf-8")
        os.replace(tmp, path)
    return {"settings": str(path), "changed": raw != new,
            "backup": str(backup) if backup else None,
            "installed": hooks_installed(path)}


# ---------------------------------------------------------------------- CLI

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Manager mode: the main model plans and reviews, the "
                    "local worker writes code. See README.")
    p.add_argument("action", choices=["on", "off", "status", "list", "role",
                                      "hook", "install", "uninstall"])
    p.add_argument("--project", default=None,
                   help="project directory (default: current directory;"
                        " the git root is used)")
    p.add_argument("--settings", default=None,
                   help="Claude Code settings file for install/uninstall"
                        " (default: $CLAUDE_CONFIG_DIR or ~/.claude,"
                        " settings.json)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    if a.action == "hook":
        try:
            event = json.load(sys.stdin)
            if not isinstance(event, dict):
                return 0
            rc, out, err = handle_hook(event)
        except Exception:  # noqa: BLE001 — fail-open: never wedge a session
            return 0
        if out:
            print(out)
        if err:
            print(err, file=sys.stderr)
        return rc
    if a.action == "on":
        doc = enable(a.project, by="cli")
        print(json.dumps({**doc, "enabled": True,
                          "hooks_installed": hooks_installed(
                              settings_path(a.settings))},
                         ensure_ascii=False))
        return 0
    if a.action == "off":
        root = project_root(a.project)
        print(json.dumps({"project": str(root), "enabled": False,
                          "was_enabled": disable(root)}, ensure_ascii=False))
        return 0
    if a.action == "status":
        root = project_root(a.project)
        doc = state(root)
        print(json.dumps({"project": str(root), "enabled": doc is not None,
                          "enabled_at": (doc or {}).get("enabled_at"),
                          "hooks_installed": hooks_installed(
                              settings_path(a.settings))},
                         ensure_ascii=False))
        return 0
    if a.action == "list":
        print(json.dumps(list_enabled(), ensure_ascii=False))
        return 0
    if a.action == "role":
        print(role_text(project_root(a.project)))
        return 0
    res = install(settings_path(a.settings), remove=a.action == "uninstall")
    print(json.dumps(res, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

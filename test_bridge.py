"""End-to-end test: real MCP stdio sessions against a shared bridge.db.

Covers: five-tool surface, sender from AGENT_ID, recipient-only ack,
reply_to linking, wait_seconds long-poll, dedup_key anti-spam retry,
response_deadline persistence and clamping, atomic worker claims, lease
renewal/release/expiry, safe old-schema migration, and error logging without
payload leaks. Run: uv run python test_bridge.py (no network, no services).
"""

import asyncio
import os
import sqlite3
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

HERE = Path(__file__).resolve().parent
SERVER = str(HERE / "server.py")
DB = str(HERE / "bridge.db")


def params(agent: str) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable,
        args=[SERVER],
        env={**os.environ, "AGENT_ID": agent},
    )


async def call(session: ClientSession, name: str, **arguments):
    result = await session.call_tool(name, arguments)
    if result.is_error:
        raise RuntimeError(f"{name} failed: {result.content}")
    data = result.structured_content
    if isinstance(data, dict) and set(data) == {"result"}:
        data = data["result"]
    return data


def seed_old_schema_db() -> None:
    """Recreate bridge.db in the pre-migration schema, with one old row."""
    for suffix in ("", "-wal", "-shm"):
        p = Path(DB + suffix)
        if p.exists():
            p.unlink()
    con = sqlite3.connect(DB)
    con.execute(
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            recipient TEXT NOT NULL,
            text TEXT NOT NULL,
            reply_to INTEGER,
            created_at REAL NOT NULL,
            acknowledged INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    con.execute(
        "INSERT INTO messages (sender, recipient, text, reply_to, created_at, acknowledged)"
        " VALUES ('kimi', 'claude', 'old row', NULL, ?, 1)",
        (time.time(),),
    )
    con.commit()
    con.close()


def check_migration() -> None:
    con = sqlite3.connect(DB)
    columns = {row[1] for row in con.execute("PRAGMA table_info(messages)")}
    assert {"response_deadline", "dedup_key", "claimed_by", "claim_expires_at"} <= columns, columns
    indexed = {
        row[1]
        for row in con.execute("PRAGMA index_list(messages)")
    }
    assert "idx_messages_dedup" in indexed, indexed
    rows = con.execute("SELECT text, acknowledged FROM messages WHERE id = 1").fetchall()
    con.close()
    assert rows == [("old row", 1)], rows
    print("0. old bridge.db migrated in place: columns + UNIQUE(sender, dedup_key) added, row kept")


async def main() -> None:
    # Remove the previous run's error log so step 9 assertions can't pass on
    # stale entries. Server processes open it in append mode at import, so
    # this must happen before the first spawn.
    err_log = HERE / "errors.log"
    if err_log.exists():
        err_log.unlink()
    seed_old_schema_db()
    # Server processes run _init_db() at import: that performs the migration.
    async with AsyncExitStack() as stack:
        rk, wk = await stack.enter_async_context(stdio_client(params("kimi")))
        rc, wc = await stack.enter_async_context(stdio_client(params("claude")))
        kimi = await stack.enter_async_context(ClientSession(rk, wk))
        claude = await stack.enter_async_context(ClientSession(rc, wc))
        await kimi.initialize()
        await claude.initialize()
    check_migration()

    async with AsyncExitStack() as stack:
        rk, wk = await stack.enter_async_context(stdio_client(params("kimi")))
        rc, wc = await stack.enter_async_context(stdio_client(params("claude")))
        rc2, wc2 = await stack.enter_async_context(stdio_client(params("claude")))
        kimi = await stack.enter_async_context(ClientSession(rk, wk))
        claude = await stack.enter_async_context(ClientSession(rc, wc))
        claude_worker = await stack.enter_async_context(ClientSession(rc2, wc2))
        await kimi.initialize()
        await claude.initialize()
        await claude_worker.initialize()

        tools = sorted(t.name for t in (await kimi.list_tools()).tools)
        assert tools == [
            "claim_message", "mark_read", "receive_messages",
            "release_message", "send_message",
        ], tools
        print("1. exactly five tools exposed:", tools)

        r = await call(kimi, "send_message", recipient="claude", text="hello claude")
        m1 = r["message_id"]
        assert r["sender"] == "kimi" and r["duplicate"] is False
        print(f"2. kimi -> claude sent, message_id={m1}, sender from AGENT_ID")

        r = await call(claude, "receive_messages")
        assert [m["text"] for m in r["messages"]] == ["hello claude"]
        assert r["messages"][0]["reply_to"] is None and r["cursor"] == m1
        print("3. claude received unread message, cursor =", r["cursor"])

        r = await call(kimi, "mark_read", message_ids=[m1])
        assert r["acknowledged"] == 0, "sender must not acknowledge"
        r = await call(claude, "mark_read", message_ids=[m1])
        assert r["acknowledged"] == 1
        assert (await call(claude, "receive_messages"))["messages"] == []
        print("4. only recipient acknowledges; re-read after mark_read is empty")

        # dedup_key: retry must not create a second row; deadline clamps to 3600s
        before = time.time()
        r1 = await call(
            kimi, "send_message", recipient="claude", text="task report v1",
            dedup_key="bootstrap-agent-bridge-v1", response_timeout_seconds=99999,
        )
        r2 = await call(
            kimi, "send_message", recipient="claude", text="task report v1 retry",
            dedup_key="bootstrap-agent-bridge-v1", response_timeout_seconds=99999,
        )
        assert r2["duplicate"] is True and r2["message_id"] == r1["message_id"]
        assert abs((r1["response_deadline"] - before) - 3600) < 5, "timeout must cap at 3600"
        r = await call(claude, "receive_messages")
        assert [m["text"] for m in r["messages"]] == ["task report v1"], "retry must not insert"
        assert abs(r["messages"][0]["response_deadline"] - r1["response_deadline"]) < 1e-6
        print(f"5. dedup retry returned existing id={r2['message_id']} duplicate=true, "
              f"deadline capped at +3600s and delivered")
        assert (await call(claude, "mark_read", message_ids=[r1["message_id"]]))["acknowledged"] == 1

        r = await call(kimi, "send_message", recipient="claude", text="floor",
                       response_timeout_seconds=0)
        assert 0 < r["response_deadline"] - time.time() <= 2, "timeout must floor at 1s"
        await call(claude, "mark_read", message_ids=[r["message_id"]])

        r = await call(claude, "send_message", recipient="kimi", text="reply", reply_to=m1)
        m2 = r["message_id"]
        r = await call(kimi, "receive_messages")
        msg = r["messages"][0]
        assert msg["id"] == m2 and msg["reply_to"] == m1 and msg["sender"] == "claude"
        print(f"6. reply_to links answer {m2} -> request {m1}")
        assert (await call(kimi, "mark_read", message_ids=[m2]))["acknowledged"] == 1

        waiter = asyncio.create_task(call(kimi, "receive_messages", wait_seconds=5))
        await asyncio.sleep(0.6)
        await call(claude, "send_message", recipient="kimi", text="late")
        r = await asyncio.wait_for(waiter, timeout=6)
        assert any(m["text"] == "late" for m in r["messages"])
        print("7. wait_seconds long-poll delivered a message sent 0.6s later")

        r = await call(kimi, "receive_messages")
        assert (await call(kimi, "mark_read", message_ids=[m["id"] for m in r["messages"]]))[
            "acknowledged"
        ] == len(r["messages"])
        assert (await call(kimi, "receive_messages"))["messages"] == []
        assert (await call(claude, "receive_messages"))["messages"] == []
        print("8. both inboxes drained; nothing re-delivered")

        # 9. Error path: a failing tool still returns an MCP error to the
        # caller; errors.log gets tool name + exception type + traceback, but
        # never the payload. (self-send is the cheapest guaranteed failure)
        SENTINEL = "SENTINEL-PAYLOAD-MUST-NOT-BE-LOGGED-4c8a"
        result = await kimi.call_tool(
            "send_message", {"recipient": "kimi", "text": SENTINEL}
        )
        assert result.is_error, "self-send must surface as an MCP error"
        log_text = err_log.read_text(encoding="utf-8")
        assert "send_message" in log_text, "tool name missing from errors.log"
        assert "agent_id" in log_text and "kimi" in log_text, "AGENT_ID missing from errors.log"
        assert "ValueError" in log_text, "exception type missing from errors.log"
        assert "Traceback (most recent call last)" in log_text, "traceback missing"
        assert SENTINEL not in log_text, "payload text leaked into errors.log"
        print("9. tool error -> MCP error to caller; errors.log has tool/AGENT_ID/"
              "exception/traceback and no payload")

        r = await call(kimi, "send_message", recipient="claude", text="claim race")
        claim_id = r["message_id"]
        claims = await asyncio.gather(
            call(claude, "claim_message", message_id=claim_id),
            call(claude_worker, "claim_message", message_id=claim_id),
        )
        assert sum(c["claimed"] for c in claims) == 1, claims
        winner = 0 if claims[0]["claimed"] else 1
        owner = (claude, claude_worker)[winner]
        other = (claude, claude_worker)[1 - winner]
        won = claims[winner]
        lost = claims[1 - winner]
        assert won["status"] == "claimed" and lost["status"] == "claimed_by_other"
        assert won["message"]["owned_by_current_worker"] is True
        owner_inbox = await call(owner, "receive_messages")
        other_inbox = await call(other, "receive_messages")
        assert [m["id"] for m in owner_inbox["messages"]] == [claim_id]
        assert owner_inbox["messages"][0]["owned_by_current_worker"] is True
        assert other_inbox["messages"] == []
        assert (await call(other, "mark_read", message_ids=[claim_id]))["acknowledged"] == 0
        released = await call(other, "release_message", message_id=claim_id)
        assert released["released"] is False and released["status"] == "claimed_by_other"
        print("10. concurrent claims have one owner; other worker cannot see/ack/release")

        before = time.time()
        renewed = await call(owner, "claim_message", message_id=claim_id,
                             lease_seconds=99999)
        assert renewed["claimed"] is True
        assert renewed["claim_expires_at"] > won["claim_expires_at"]
        assert abs((renewed["claim_expires_at"] - before) - 3600) < 5
        released = await call(owner, "release_message", message_id=claim_id)
        assert released["released"] is True
        repeated = await call(owner, "release_message", message_id=claim_id)
        assert repeated["released"] is False and repeated["status"] == "not_claimed"
        other_inbox = await call(other, "receive_messages")
        assert [m["id"] for m in other_inbox["messages"]] == [claim_id]
        assert other_inbox["messages"][0]["owned_by_current_worker"] is False
        assert other_inbox["messages"][0]["claim_expires_at"] is None
        before = time.time()
        second_claim = await call(other, "claim_message", message_id=claim_id,
                                  lease_seconds=0)
        assert second_claim["claimed"] is True
        assert 29 <= second_claim["claim_expires_at"] - before <= 31
        print("11. owner renews lease (3600s cap), releases; peer claims (30s floor)")

        con = sqlite3.connect(DB)
        con.execute("UPDATE messages SET claim_expires_at = ? WHERE id = ?",
                    (time.time() - 1, claim_id))
        con.commit()
        con.close()
        reclaimed = await call(owner, "claim_message", message_id=claim_id)
        assert reclaimed["claimed"] is True
        assert (await call(other, "mark_read", message_ids=[claim_id]))["acknowledged"] == 0
        assert (await call(owner, "mark_read", message_ids=[claim_id]))["acknowledged"] == 1
        completed = await call(other, "claim_message", message_id=claim_id)
        assert completed["claimed"] is False and completed["status"] == "acknowledged"
        missing = await call(owner, "claim_message", message_id=999999)
        assert missing["claimed"] is False and missing["status"] == "not_found"
        assert (await call(claude, "receive_messages"))["messages"] == []
        assert (await call(claude_worker, "receive_messages"))["messages"] == []
        print("12. expired lease is reclaimable; only owner acks; terminal states reported")

    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())

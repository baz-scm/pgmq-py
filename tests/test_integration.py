"""Integration tests for pgmq-py."""

from typing import TypedDict

import pytest

from pgmq_py import PGMQ, Message


class TestMessage(TypedDict, total=False):
    org: str
    repo: str
    metadata: dict[str, str | int]
    group_id: str


class TestSendMessage:
    async def test_send_message(self, pgmq: PGMQ, test_queue: str) -> None:
        msg_id = await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        assert isinstance(msg_id, int)
        assert msg_id > 0


class TestReadMessage:
    async def test_read_message(self, pgmq: PGMQ, test_queue: str) -> None:
        await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        msg = await pgmq.read_message(test_queue, vt=60)
        assert msg is not None
        assert msg.message["org"] == "acme"

    async def test_read_empty_queue(self, pgmq: PGMQ, test_queue: str) -> None:
        msg = await pgmq.read_message(test_queue, vt=60)
        assert msg is None


class TestDeleteMessage:
    async def test_delete_message(self, pgmq: PGMQ, test_queue: str) -> None:
        msg_id = await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        deleted_id = await pgmq.delete_message(test_queue, msg_id)
        assert deleted_id == msg_id

        # Verify message is deleted
        msg = await pgmq.read_message(test_queue, vt=60)
        assert msg is None


class TestArchiveMessage:
    async def test_archive_message(self, pgmq: PGMQ, test_queue: str) -> None:
        msg_id = await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        archived_id = await pgmq.archive_message(test_queue, msg_id)
        assert archived_id == msg_id

        # Verify message is no longer in queue
        msg = await pgmq.read_message(test_queue, vt=60)
        assert msg is None


class TestQueueInterface:
    async def test_queue_read_message(self, pgmq: PGMQ, test_queue: str) -> None:
        await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        queue = pgmq.get_queue(test_queue)
        msg = await queue.read_message(vt=60)
        assert msg is not None
        assert msg.message["org"] == "acme"

    async def test_queue_delete_message(self, pgmq: PGMQ, test_queue: str) -> None:
        msg_id = await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        queue = pgmq.get_queue(test_queue)
        deleted_id = await queue.delete_message(msg_id)
        assert deleted_id == msg_id

    async def test_queue_archive_message(self, pgmq: PGMQ, test_queue: str) -> None:
        msg_id = await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo", "metadata": {"site": "google.com"}},
            vt=0,
        )
        queue = pgmq.get_queue(test_queue)
        archived_id = await queue.archive_message(msg_id)
        assert archived_id == msg_id


class TestMessageType:
    async def test_message_fields(self, pgmq: PGMQ, test_queue: str) -> None:
        await pgmq.send_message(
            test_queue,
            {"org": "acme", "repo": "repo"},
            vt=0,
        )
        msg = await pgmq.read_message(test_queue, vt=60)
        assert msg is not None
        assert isinstance(msg, Message)
        assert isinstance(msg.msg_id, int)
        assert isinstance(msg.read_count, int)
        assert msg.read_count >= 1
        assert msg.enqueued_at is not None
        assert msg.vt is not None

    async def test_message_is_frozen(self, pgmq: PGMQ, test_queue: str) -> None:
        await pgmq.send_message(test_queue, {"test": "data"}, vt=0)
        msg = await pgmq.read_message(test_queue, vt=60)
        assert msg is not None

        with pytest.raises(AttributeError):
            msg.msg_id = 999  # type: ignore[misc]


class TestPoolKwargs:
    async def test_check_kwarg_runs_on_checkout(self, database_url: str) -> None:
        # Assert the observable behavior the check kwarg buys us: the callable
        # is run every time a connection is handed out. This is what guards
        # against server-side-killed connections, and is stable across
        # psycopg_pool versions (unlike poking at private pool attributes).
        calls = 0

        async def recording_check(conn: object) -> None:
            nonlocal calls
            calls += 1

        async with PGMQ(database_url, check=recording_check) as pgmq:
            pool = pgmq._get_pool()
            async with pool.connection() as conn:
                await conn.execute("SELECT 1")
            async with pool.connection() as conn:
                await conn.execute("SELECT 1")
        assert calls == 2

    async def test_no_extra_kwargs(self, database_url: str) -> None:
        async with PGMQ(database_url) as pgmq:
            assert pgmq._pool is not None

    def test_reserved_kwarg_collision_raises(self) -> None:
        # Raised in __init__ before any connection, so no DB needed.
        with pytest.raises(TypeError):
            PGMQ("postgresql://localhost/db", open=False)

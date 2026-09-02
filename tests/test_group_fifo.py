"""Group FIFO pattern tests for pgmq-py."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from pgmq_py import PGMQ


@pytest.fixture
async def group_fifo_queue(pgmq: PGMQ) -> AsyncGenerator[str, None]:
    """Create a queue for group FIFO tests."""
    queue_name = f"group_fifo_{uuid.uuid4().hex[:8]}"
    await pgmq.create_queue(queue_name)
    yield queue_name
    await pgmq.delete_queue(queue_name)


class TestReadMessageByGroupId:
    async def test_return_null_on_empty_queue(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        msg = await pgmq.read_message_by_group_id(group_fifo_queue, ["group_id"], vt=60)
        assert msg is None

    async def test_read_oldest_available_message(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo1", "group_id": "123"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo2", "group_id": "456"},
            vt=0,
        )

        msg = await pgmq.read_message_by_group_id(group_fifo_queue, ["group_id"], vt=60)
        assert msg is not None
        assert msg.message["repo"] == "repo1"

    async def test_skip_groups_where_oldest_message_is_in_progress(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        # Create messages: 2 for group 123, 1 for group 456
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo1", "group_id": "123"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo2", "group_id": "123"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo3", "group_id": "456"},
            vt=0,
        )

        # Read and lock the first message (repo1, group_id=123)
        msg1 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg1 is not None
        assert msg1.message["repo"] == "repo1"
        assert msg1.message["group_id"] == "123"

        # Now try to read again - should skip group_id=123 entirely and get group_id=456
        msg2 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg2 is not None
        assert msg2.message["group_id"] == "456"
        assert msg2.message["repo"] == "repo3"

        # Third read should return None (both groups have their oldest msg in progress)
        msg3 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg3 is None

    async def test_maintain_fifo_within_single_group(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "first", "group_id": "999"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "second", "group_id": "999"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "third", "group_id": "999"},
            vt=0,
        )

        msg1 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg1 is not None
        assert msg1.message["repo"] == "first"
        await pgmq.delete_message(group_fifo_queue, msg1.msg_id)

        msg2 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg2 is not None
        assert msg2.message["repo"] == "second"
        await pgmq.delete_message(group_fifo_queue, msg2.msg_id)

        msg3 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg3 is not None
        assert msg3.message["repo"] == "third"

    async def test_handle_multiple_groups_in_parallel(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "g1_m1", "group_id": "g1"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "g1_m2", "group_id": "g1"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "g2_m1", "group_id": "g2"},
            vt=0,
        )

        msg1 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg1 is not None
        assert msg1.message["group_id"] == "g1"
        assert msg1.message["repo"] == "g1_m1"

        # While g1 is in progress, g2 can still be processed
        msg2 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg2 is not None
        assert msg2.message["group_id"] == "g2"
        assert msg2.message["repo"] == "g2_m1"

        # Clean up
        await pgmq.delete_message(group_fifo_queue, msg1.msg_id)
        await pgmq.delete_message(group_fifo_queue, msg2.msg_id)

        # Now g1_m2 should be available
        msg3 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["group_id"], vt=60
        )
        assert msg3 is not None
        assert msg3.message["group_id"] == "g1"
        assert msg3.message["repo"] == "g1_m2"


class TestReadAllMessagesByGroupId:
    async def test_read_all_messages_for_group_in_fifo_order(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo1", "group_id": "123"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo2", "group_id": "123"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "repo3", "group_id": "456"},
            vt=0,
        )

        msgs = await pgmq.read_all_messages_by_group_id(
            group_fifo_queue, ["group_id"], "123", vt=60
        )
        assert len(msgs) == 2
        assert msgs[0].message["group_id"] == "123"
        assert msgs[1].message["group_id"] == "123"
        assert msgs[0].message["repo"] == "repo1"
        assert msgs[1].message["repo"] == "repo2"
        # Verify FIFO: msg_id should be ascending
        assert msgs[0].msg_id < msgs[1].msg_id

    async def test_read_messages_with_future_vt(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "future1", "group_id": "future"},
            vt=3600,  # 1 hour in future
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "future2", "group_id": "future"},
            vt=3600,
        )

        msgs = await pgmq.read_all_messages_by_group_id(
            group_fifo_queue, ["group_id"], "future", vt=60
        )
        assert len(msgs) == 2  # Should get both even though vt is in future

    async def test_return_empty_for_nonexistent_group(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        msgs = await pgmq.read_all_messages_by_group_id(
            group_fifo_queue, ["group_id"], "nonexistent", vt=60
        )
        assert len(msgs) == 0


class TestDeleteMessagesByIds:
    async def test_delete_multiple_messages(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "del1", "group_id": "del"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "del2", "group_id": "del"},
            vt=0,
        )

        msgs = await pgmq.read_all_messages_by_group_id(
            group_fifo_queue, ["group_id"], "del", vt=60
        )
        ids = [m.msg_id for m in msgs]
        deleted_ids = await pgmq.delete_messages_by_ids(group_fifo_queue, ids)

        assert len(deleted_ids) == 2
        assert set(deleted_ids) == set(ids)

        # Verify messages are deleted
        after_msgs = await pgmq.read_all_messages_by_group_id(
            group_fifo_queue, ["group_id"], "del", vt=60
        )
        assert len(after_msgs) == 0

    async def test_handle_empty_ids_array(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        deleted_ids = await pgmq.delete_messages_by_ids(group_fifo_queue, [])
        assert len(deleted_ids) == 0


class TestNestedJsonPaths:
    async def test_nested_json_paths(self, pgmq: PGMQ, group_fifo_queue: str) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"org": "nested1", "repo": "test1", "metadata": {"site": "site1.com"}},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"org": "nested2", "repo": "test2", "metadata": {"site": "site1.com"}},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"org": "nested3", "repo": "test3", "metadata": {"site": "site2.com"}},
            vt=0,
        )

        msg = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["metadata", "site"], vt=60
        )
        assert msg is not None
        assert msg.message["repo"] == "test1"  # oldest message

        # Lock site1.com group, should get site2.com
        msg2 = await pgmq.read_message_by_group_id(
            group_fifo_queue, ["metadata", "site"], vt=60
        )
        assert msg2 is not None
        assert msg2.message["metadata"]["site"] == "site2.com"


class TestQueueInterfaceGroupFifo:
    async def test_queue_read_message_by_group_id(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "q1", "group_id": "q1"},
            vt=0,
        )

        queue = pgmq.get_queue(group_fifo_queue)
        msg = await queue.read_message_by_group_id(["group_id"], vt=60)
        assert msg is not None
        assert msg.message["group_id"] == "q1"

    async def test_queue_read_all_messages_by_group_id(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "q2_1", "group_id": "q2"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "q2_2", "group_id": "q2"},
            vt=0,
        )

        queue = pgmq.get_queue(group_fifo_queue)
        msgs = await queue.read_all_messages_by_group_id(["group_id"], "q2", vt=60)
        assert len(msgs) == 2
        assert msgs[0].message["group_id"] == "q2"

    async def test_queue_delete_messages_by_ids(
        self, pgmq: PGMQ, group_fifo_queue: str
    ) -> None:
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "q3_1", "group_id": "q3"},
            vt=0,
        )
        await pgmq.send_message(
            group_fifo_queue,
            {"repo": "q3_2", "group_id": "q3"},
            vt=0,
        )

        queue = pgmq.get_queue(group_fifo_queue)
        msgs = await queue.read_all_messages_by_group_id(["group_id"], "q3", vt=60)
        ids = [m.msg_id for m in msgs]
        deleted_ids = await queue.delete_messages_by_ids(ids)
        assert len(deleted_ids) == 2

"""Queue wrapper class for pgmq-py."""

import json
from datetime import datetime
from typing import Any, TypeVar

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from .queries import (
    archive_query,
    delete_messages_by_ids_query,
    delete_query,
    read_all_messages_by_group_id_query,
    read_message_by_group_id_query,
    read_query,
    send_query,
    vt_query,
)
from .types import Message, parse_db_message
from .utils import execute_with_transaction, sanitize_nul, validate_queue_name

T = TypeVar("T")


class Queue:
    """A wrapper class for queue-specific operations.

    Provides the same message operations as PGMQ but with a bound queue name.
    """

    def __init__(
        self, pool: AsyncConnectionPool[AsyncConnection[Any]], name: str
    ) -> None:
        """Initialize a Queue.

        Args:
            pool: The connection pool to use.
            name: The queue name.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        validate_queue_name(name)
        self._pool = pool
        self._name = name

    @property
    def name(self) -> str:
        """Get the queue name."""
        return self._name

    async def send_message(
        self, message: Any, vt: int = 0, sanitize: bool = False
    ) -> int:
        """Send a message to the queue.

        Args:
            message: The message payload (will be JSON serialized).
            vt: Visibility timeout in seconds. The message will be hidden
                from consumers for this duration after being sent.
            sanitize: If True, replace raw NUL bytes (``\\x00``) in the
                payload with their literal escape text (``\\u0000``) before
                serialization. Postgres jsonb cannot store the U+0000 code
                point, so enable this when payloads may contain raw NUL bytes.

        Returns:
            The message ID.
        """
        if sanitize:
            message = sanitize_nul(message)
        query = send_query(self._name, vt)
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, [json.dumps(message)])
                row = await cur.fetchone()
                if row is None:
                    raise RuntimeError("Failed to send message")
                return int(row[0])

    async def read_message(self, vt: int = 0) -> Message[Any] | None:
        """Read a message from the queue.

        Args:
            vt: Visibility timeout in seconds. The message will be hidden from
                other consumers for this duration.

        Returns:
            The message if available, None otherwise.
        """
        query = read_query(self._name, vt)
        rows = await execute_with_transaction(self._pool, query)
        if rows:
            return parse_db_message(rows[0])
        return None

    async def delete_message(self, msg_id: int) -> int:
        """Delete a message from the queue.

        Args:
            msg_id: The message ID to delete.

        Returns:
            The deleted message ID.
        """
        query = delete_query(self._name, msg_id)
        rows = await execute_with_transaction(self._pool, query)
        return int(rows[0]["msg_id"])

    async def archive_message(self, msg_id: int) -> int:
        """Archive a message from the queue.

        Moves the message from the queue to the archive table.

        Args:
            msg_id: The message ID to archive.

        Returns:
            The archived message ID.
        """
        query = archive_query(self._name, msg_id)
        rows = await execute_with_transaction(self._pool, query)
        return int(rows[0]["msg_id"])

    async def set_vt(self, msg_id: int, vt: datetime) -> int:
        """Set the visibility time for a message.

        Args:
            msg_id: The message ID whose visibility time to update.
            vt: The new visibility timestamp.

        Returns:
            The updated message ID.
        """
        query = vt_query(self._name, msg_id, vt)
        rows = await execute_with_transaction(self._pool, query)
        return int(rows[0]["msg_id"])

    async def read_message_by_group_id(
        self, group_id_path: list[str], vt: int
    ) -> Message[Any] | None:
        """Read a message using the Group FIFO pattern.

        Returns the single oldest available message across all groups where
        the oldest message is not in progress. If a group's oldest message
        is in progress (vt in future), that entire group is skipped.

        Args:
            group_id_path: JSON path to the group ID field
                (e.g., ['pr_id'] or ['metadata', 'group_id']).
            vt: Visibility timeout in seconds.

        Returns:
            The oldest available message, or None if none available.
        """
        json_path = "{" + ",".join(group_id_path) + "}"
        query = read_message_by_group_id_query(self._name, vt)
        rows = await execute_with_transaction(self._pool, query, [json_path, json_path])
        if rows:
            return parse_db_message(rows[0])
        return None

    async def read_all_messages_by_group_id(
        self, group_id_path: list[str], group_id_value: str, vt: int
    ) -> list[Message[Any]]:
        """Read all messages for a specific group ID.

        Ignores visibility timeout and returns all messages for the group,
        ordered by msg_id. Use this when you want to process all remaining
        messages for a group you're already working on.

        Args:
            group_id_path: JSON path to the group ID field.
            group_id_value: The value of the group ID to filter by.
            vt: Visibility timeout to set for all messages.

        Returns:
            List of all messages for this group.
        """
        json_path = "{" + ",".join(group_id_path) + "}"
        query = read_all_messages_by_group_id_query(self._name, vt)
        rows = await execute_with_transaction(
            self._pool, query, [json_path, group_id_value]
        )
        return [parse_db_message(row) for row in rows]

    async def delete_messages_by_ids(self, ids: list[int]) -> list[int]:
        """Delete multiple messages by their IDs.

        Args:
            ids: List of message IDs to delete.

        Returns:
            List of deleted message IDs.
        """
        query = delete_messages_by_ids_query(self._name)
        rows = await execute_with_transaction(self._pool, query, [ids])
        return [int(row["msg_id"]) for row in rows]

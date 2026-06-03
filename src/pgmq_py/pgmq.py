"""Main PGMQ class for pgmq-py."""

from types import TracebackType
from typing import Any, Self, TypeVar

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from .queries import (
    create_queue_query,
    create_schema_query,
    delete_queue_query,
    delete_schema_query,
)
from .queue import Queue
from .types import Message
from .utils import validate_queue_name

T = TypeVar("T")


class PGMQ:
    """PostgreSQL Message Queue client.

    This is the central class for interacting with pgmq. It manages the
    connection pool and provides methods for schema, queue, and message
    operations.

    Usage:
        async with PGMQ("postgresql://user:pass@localhost/db") as pgmq:
            await pgmq.create_schema()
            await pgmq.create_queue("my_queue")
            await pgmq.send_message("my_queue", {"data": "value"})
    """

    def __init__(
        self,
        connection_string: str,
        min_size: int = 1,
        max_size: int = 10,
        **pool_kwargs: Any,
    ):
        """Initialize PGMQ.

        Args:
            connection_string: PostgreSQL connection string.
            min_size: Minimum number of connections in the pool.
            max_size: Maximum number of connections in the pool.
            **pool_kwargs: Extra keyword arguments forwarded to
                psycopg_pool.AsyncConnectionPool (e.g. check, max_lifetime,
                configure, reset).

        Raises:
            TypeError: If min_size, max_size, or open are also passed via
                pool_kwargs.
        """
        for key in ("min_size", "max_size", "open"):
            if key in pool_kwargs:
                raise TypeError(f"{key!r} must not be passed via pool_kwargs")
        self._connection_string = connection_string
        self._pool: AsyncConnectionPool[AsyncConnection[Any]] | None = None
        self._min_size = min_size
        self._max_size = max_size
        self._pool_kwargs = pool_kwargs

    async def __aenter__(self) -> Self:
        """Enter the async context manager."""
        self._pool = AsyncConnectionPool(
            self._connection_string,
            min_size=self._min_size,
            max_size=self._max_size,
            open=False,
            **self._pool_kwargs,
        )
        await self._pool.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    def _get_pool(self) -> AsyncConnectionPool[AsyncConnection[Any]]:
        """Get the connection pool.

        Raises:
            RuntimeError: If PGMQ is not used as a context manager.
        """
        if self._pool is None:
            raise RuntimeError("PGMQ must be used as an async context manager")
        return self._pool

    # Schema operations

    async def create_schema(self) -> None:
        """Create the pgmq schema if it doesn't exist."""
        pool = self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(create_schema_query())

    async def delete_schema(self) -> None:
        """Delete the pgmq schema if it exists."""
        pool = self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(delete_schema_query())

    # Queue operations

    async def create_queue(self, name: str) -> None:
        """Create a queue and its archive table.

        Args:
            name: The queue name. Must be alphanumeric plus underscore,
                max 47 characters.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        validate_queue_name(name)
        pool = self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(create_queue_query(name))

    async def delete_queue(self, name: str) -> None:
        """Delete a queue and its archive table.

        Args:
            name: The queue name.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        validate_queue_name(name)
        pool = self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(delete_queue_query(name))

    def get_queue(self, name: str) -> Queue:
        """Get a Queue instance for the given queue name.

        Args:
            name: The queue name.

        Returns:
            A Queue instance bound to the given name.
        """
        return Queue(self._get_pool(), name)

    # Message operations

    async def send_message(
        self, queue: str, message: Any, vt: int = 0, sanitize: bool = False
    ) -> int:
        """Send a message to a queue.

        Args:
            queue: The queue name.
            message: The message payload (will be JSON serialized).
            vt: Visibility timeout in seconds. The message will be hidden
                from consumers for this duration after being sent.
            sanitize: If True, replace raw NUL bytes (``\\x00``) in the
                payload with their literal escape text (``\\u0000``) before
                serialization. Postgres jsonb cannot store the U+0000 code
                point, so enable this when payloads may contain raw NUL bytes.

        Returns:
            The message ID.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        return await self.get_queue(queue).send_message(message, vt, sanitize)

    async def read_message(self, queue: str, vt: int) -> Message[Any] | None:
        """Read a message from a queue.

        Args:
            queue: The queue name.
            vt: Visibility timeout in seconds. The message will be hidden
                from other consumers for this duration.

        Returns:
            The message if available, None otherwise.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        return await self.get_queue(queue).read_message(vt)

    async def delete_message(self, queue: str, msg_id: int) -> int:
        """Delete a message from a queue.

        Args:
            queue: The queue name.
            msg_id: The message ID to delete.

        Returns:
            The deleted message ID.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        return await self.get_queue(queue).delete_message(msg_id)

    async def archive_message(self, queue: str, msg_id: int) -> int:
        """Archive a message from a queue.

        Moves the message from the queue to the archive table.

        Args:
            queue: The queue name.
            msg_id: The message ID to archive.

        Returns:
            The archived message ID.

        Raises:
            QueueNameError: If the queue name is invalid.
        """
        return await self.get_queue(queue).archive_message(msg_id)

    # Group FIFO operations

    async def read_message_by_group_id(
        self, queue: str, group_id_path: list[str], vt: int
    ) -> Message[Any] | None:
        """Read a message using the Group FIFO pattern.

        Returns the single oldest available message across all groups where
        the oldest message is not in progress. If a group's oldest message
        is in progress (vt in future), that entire group is skipped.

        This allows parallel processing of different groups while maintaining
        FIFO ordering within each group.

        Args:
            queue: The queue name.
            group_id_path: JSON path to the group ID field
                (e.g., ['pr_id'] or ['metadata', 'group_id']).
            vt: Visibility timeout in seconds.

        Returns:
            The oldest available message, or None if none available.
        """
        return await self.get_queue(queue).read_message_by_group_id(group_id_path, vt)

    async def read_all_messages_by_group_id(
        self, queue: str, group_id_path: list[str], group_id_value: str, vt: int
    ) -> list[Message[Any]]:
        """Read all messages for a specific group ID.

        Ignores visibility timeout and returns all messages for the group,
        ordered by msg_id. Use this when you want to process all remaining
        messages for a group you're already working on.

        Args:
            queue: The queue name.
            group_id_path: JSON path to the group ID field.
            group_id_value: The value of the group ID to filter by.
            vt: Visibility timeout to set for all messages.

        Returns:
            List of all messages for this group.
        """
        return await self.get_queue(queue).read_all_messages_by_group_id(
            group_id_path, group_id_value, vt
        )

    async def delete_messages_by_ids(self, queue: str, ids: list[int]) -> list[int]:
        """Delete multiple messages by their IDs.

        Args:
            queue: The queue name.
            ids: List of message IDs to delete.

        Returns:
            List of deleted message IDs.
        """
        return await self.get_queue(queue).delete_messages_by_ids(ids)

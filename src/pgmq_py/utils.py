"""Utility functions for pgmq-py."""

import re
from collections.abc import Sequence
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

NAMELEN = 64
BIGGEST_CONCAT = "archived_at_idx_"
MAX_PGMQ_QUEUE_LEN = NAMELEN - 1 - len(BIGGEST_CONCAT)


class QueueNameError(Exception):
    """Raised when a queue name is invalid."""

    pass


def validate_queue_name(name: str) -> None:
    """Validate a queue name.

    Queue names must be alphanumeric plus underscore, and max 47 characters.

    Args:
        name: The queue name to validate.

    Raises:
        QueueNameError: If the queue name is invalid.
    """
    if len(name) > MAX_PGMQ_QUEUE_LEN:
        raise QueueNameError(
            f"Queue name is too long. Max length is {MAX_PGMQ_QUEUE_LEN} characters."
        )

    if not re.match(r"^[a-zA-Z0-9_]+$", name):
        raise QueueNameError(
            "Queue name must contain only alphanumeric characters and underscores"
        )


async def execute_with_transaction(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    query: str,
    params: Sequence[Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute a query with transaction handling.

    Args:
        pool: The connection pool to use.
        query: The SQL query to execute.
        params: Optional query parameters.

    Returns:
        List of rows as dictionaries.
    """
    async with pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                if params:
                    await cur.execute(query, params)
                else:
                    await cur.execute(query)

                if cur.description is None:
                    return []

                columns = [desc[0] for desc in cur.description]
                rows = await cur.fetchall()
                return [dict(zip(columns, row, strict=True)) for row in rows]

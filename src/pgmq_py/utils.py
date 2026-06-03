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


def sanitize_nul(value: Any) -> Any:
    """Replace raw NUL bytes with their literal escape text.

    Postgres text and jsonb columns cannot store the U+0000 code point, so a
    payload containing a raw NUL byte fails on insert. This helper recursively
    walks dicts, lists, and strings, replacing each raw NUL byte (``\\x00``)
    with the literal six-character escape text ``\\u0000`` (backslash, u, 0,
    0, 0, 0). The transformation is lossless and readable.

    An already-escaped literal ``\\u0000`` is left untouched: it contains no
    raw NUL byte, so it is a normal sequence of characters rather than a NUL
    code point. Values that are not dicts, lists, or strings pass through
    unchanged.

    Args:
        value: The value to sanitize.

    Returns:
        The value with raw NUL bytes replaced by literal escape text.
    """
    if isinstance(value, str):
        return value.replace("\x00", "\\u0000")
    if isinstance(value, dict):
        return {key: sanitize_nul(val) for key, val in value.items()}
    if isinstance(value, list):
        return [sanitize_nul(item) for item in value]
    return value


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

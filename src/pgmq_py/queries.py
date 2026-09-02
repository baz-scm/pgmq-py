"""SQL query builders for pgmq-py."""

from datetime import datetime

PGMQ_SCHEMA = "pgmq"
QUEUE_PREFIX = "q"
ARCHIVE_PREFIX = "a"


def create_schema_query() -> str:
    """Generate SQL to create the pgmq schema."""
    return f"CREATE SCHEMA IF NOT EXISTS {PGMQ_SCHEMA}"


def delete_schema_query() -> str:
    """Generate SQL to delete the pgmq schema."""
    return f"DROP SCHEMA IF EXISTS {PGMQ_SCHEMA} CASCADE"


def create_queue_query(name: str) -> str:
    """Generate SQL to create a queue and its archive table.

    Args:
        name: The queue name.

    Returns:
        SQL statement to create the queue and archive tables.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name}
        (
            msg_id      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            read_ct     INT                      DEFAULT 0     NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt          TIMESTAMP WITH TIME ZONE               NOT NULL,
            message     JSONB
        );
        CREATE TABLE IF NOT EXISTS {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name}
        (
            msg_id      BIGINT PRIMARY KEY,
            read_ct     INT                      DEFAULT 0     NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            archived_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt          TIMESTAMP WITH TIME ZONE               NOT NULL,
            message     JSONB
        );"""


def delete_queue_query(name: str) -> str:
    """Generate SQL to delete a queue and its archive table.

    Args:
        name: The queue name.

    Returns:
        SQL statement to drop the queue and archive tables.
    """
    return f"""
        DROP TABLE IF EXISTS {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{name};
        DROP TABLE IF EXISTS {PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{name};"""


def send_query(queue: str, vt: int) -> str:
    """Generate SQL to send a message to a queue.

    Args:
        queue: The queue name.
        vt: Visibility timeout in seconds.

    Returns:
        SQL statement to insert a message.
    """
    return f"""INSERT INTO {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue} (vt, message)
        VALUES ((now() + interval '{vt} seconds'), %s::jsonb)
        RETURNING msg_id;"""


def read_query(queue: str, vt: int) -> str:
    """Generate SQL to read a message from a queue.

    Args:
        queue: The queue name.
        vt: Visibility timeout in seconds.

    Returns:
        SQL statement to read and lock a message.
    """
    return f"""WITH cte AS
                 (SELECT msg_id
                  FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}
                  WHERE vt <= now()
                  ORDER BY msg_id
                  LIMIT 1 FOR UPDATE SKIP LOCKED)
        UPDATE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue} t
        SET vt      = now() + interval '{vt} seconds',
            read_ct = read_ct + 1
        FROM cte
        WHERE t.msg_id = cte.msg_id
        RETURNING *;"""


def archive_query(queue: str, msg_id: int) -> str:
    """Generate SQL to archive a message.

    Args:
        queue: The queue name.
        msg_id: The message ID to archive.

    Returns:
        SQL statement to move message from queue to archive.
    """
    archive_table = f"{PGMQ_SCHEMA}.{ARCHIVE_PREFIX}_{queue}"
    queue_table = f"{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}"
    return f"""WITH archived AS (
        DELETE FROM {queue_table}
            WHERE msg_id = {msg_id}
            RETURNING msg_id, vt, read_ct, enqueued_at, message)
        INSERT INTO {archive_table} (msg_id, vt, read_ct, enqueued_at, message)
        SELECT msg_id, vt, read_ct, enqueued_at, message
        FROM archived
        RETURNING msg_id;"""


def delete_query(queue: str, msg_id: int) -> str:
    """Generate SQL to delete a message.

    Args:
        queue: The queue name.
        msg_id: The message ID to delete.

    Returns:
        SQL statement to delete a message.
    """
    return f"""DELETE
        FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}
        WHERE msg_id = {msg_id}
        RETURNING msg_id;"""


def vt_query(queue: str, msg_id: int, vt: datetime) -> str:
    """Generate SQL to set the visibility time for a message.

    Args:
        queue: The queue name.
        msg_id: The message ID whose visibility time to update.
        vt: The new visibility timestamp.

    Returns:
        SQL statement to update a message's visibility time.
    """
    timestamp = vt.isoformat(sep=" ", timespec="milliseconds")
    return f"""UPDATE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}
        SET vt = '{timestamp}'::timestamptz
        WHERE msg_id = {msg_id}
        RETURNING msg_id;"""


def read_message_by_group_id_query(queue: str, vt: int) -> str:
    """Generate SQL to read a message using the Group FIFO pattern.

    Returns the single oldest available message across all groups where the
    oldest message is not in progress. If a group's oldest message is in
    progress (vt in future), that entire group is skipped.

    Args:
        queue: The queue name.
        vt: Visibility timeout in seconds.

    Returns:
        SQL statement to read by group ID.
    """
    table = f"{PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}"
    return f"""WITH cte0 AS
                 (SELECT message #>> %s AS group_field, MIN(msg_id) AS msg_id
                  FROM {table}
                  GROUP BY group_field),
             cte1 AS
                 (SELECT t1.msg_id AS msg_id
                  FROM {table} AS t1
                  JOIN cte0 AS t2
                    ON t1.message #>> %s = t2.group_field
                    AND t1.msg_id = t2.msg_id
                  WHERE vt <= clock_timestamp()
                  ORDER BY msg_id ASC
                  LIMIT 1 FOR UPDATE SKIP LOCKED)
        UPDATE {table} m
        SET vt      = clock_timestamp() + interval '{vt} seconds',
            read_ct = read_ct + 1
        FROM cte1
        WHERE m.msg_id = cte1.msg_id
        RETURNING m.*;"""


def read_all_messages_by_group_id_query(queue: str, vt: int) -> str:
    """Generate SQL to read all messages for a specific group ID.

    Ignores visibility timeout and returns all messages for the group,
    ordered by msg_id.

    Args:
        queue: The queue name.
        vt: Visibility timeout in seconds.

    Returns:
        SQL statement to read all messages by group ID.
    """
    return f"""WITH cte AS
                 (SELECT msg_id
                  FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}
                  WHERE message #>> %s = %s
                  ORDER BY msg_id
                  FOR UPDATE)
        UPDATE {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue} t
        SET vt      = now() + interval '{vt} seconds',
            read_ct = read_ct + 1
        FROM cte
        WHERE t.msg_id = cte.msg_id
        RETURNING t.*;"""


def delete_messages_by_ids_query(queue: str) -> str:
    """Generate SQL to delete multiple messages by their IDs.

    Args:
        queue: The queue name.

    Returns:
        SQL statement to delete messages by IDs.
    """
    return f"""DELETE
        FROM {PGMQ_SCHEMA}.{QUEUE_PREFIX}_{queue}
        WHERE msg_id = ANY(%s::bigint[])
        RETURNING msg_id;"""

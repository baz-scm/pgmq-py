"""Type definitions for pgmq-py."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Message(Generic[T]):
    """A message from the queue.

    Attributes:
        msg_id: Unique message identifier.
        read_count: Number of times the message has been read.
        enqueued_at: Timestamp when the message was added to the queue.
        vt: Visibility timeout - when the message becomes available again.
        message: The message payload.
    """

    msg_id: int
    read_count: int
    enqueued_at: datetime
    vt: datetime
    message: T


def parse_db_message(row: dict[str, Any]) -> Message[Any]:
    """Convert a database row to a Message object.

    Args:
        row: Database row with msg_id, read_ct, enqueued_at, vt, message fields.

    Returns:
        A Message object with the parsed data.
    """
    return Message(
        msg_id=int(row["msg_id"]),
        read_count=int(row["read_ct"]),
        enqueued_at=row["enqueued_at"],
        vt=row["vt"],
        message=row["message"],
    )

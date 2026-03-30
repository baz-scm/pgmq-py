"""pgmq-py: A PostgreSQL message queue library for Python."""

from .pgmq import PGMQ
from .queue import Queue
from .types import Message
from .utils import QueueNameError

__all__ = ["PGMQ", "Queue", "Message", "QueueNameError"]
__version__ = "0.1.0"

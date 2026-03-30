# pgmq-py

A PostgreSQL message queue library for Python with async support.

## Installation

```bash
pip install pgmq-py
```

## Quick Start

```python
import asyncio
from pgmq_py import PGMQ

async def main():
    async with PGMQ("postgresql://user:password@localhost:5432/mydb") as pgmq:
        # Create schema and queue
        await pgmq.create_schema()
        await pgmq.create_queue("my_queue")

        # Send a message
        msg_id = await pgmq.send_message("my_queue", {"hello": "world"}, vt=0)
        print(f"Sent message: {msg_id}")

        # Read a message
        msg = await pgmq.read_message("my_queue", vt=60)
        if msg:
            print(f"Received: {msg.message}")
            await pgmq.delete_message("my_queue", msg.msg_id)

asyncio.run(main())
```

## Features

- Async-only API using psycopg3
- Type-safe with full type hints
- Connection pooling
- Group FIFO pattern support for parallel processing

## API

### PGMQ Class

```python
async with PGMQ("postgresql://...") as pgmq:
    # Schema management
    await pgmq.create_schema()
    await pgmq.delete_schema()

    # Queue management
    await pgmq.create_queue("my_queue")
    await pgmq.delete_queue("my_queue")
    queue = pgmq.get_queue("my_queue")

    # Message operations
    msg_id = await pgmq.send_message("my_queue", {"data": "value"}, vt=0)
    msg = await pgmq.read_message("my_queue", vt=60)
    await pgmq.delete_message("my_queue", msg_id)
    await pgmq.archive_message("my_queue", msg_id)

    # Group FIFO operations
    msg = await pgmq.read_message_by_group_id("my_queue", ["group_id"], vt=60)
    msgs = await pgmq.read_all_messages_by_group_id("my_queue", ["group_id"], "value", vt=60)
    await pgmq.delete_messages_by_ids("my_queue", [1, 2, 3])
```

### Queue Class

```python
queue = pgmq.get_queue("my_queue")

msg = await queue.read_message(vt=60)
await queue.delete_message(msg.msg_id)
await queue.archive_message(msg.msg_id)

# Group FIFO
msg = await queue.read_message_by_group_id(["group_id"], vt=60)
msgs = await queue.read_all_messages_by_group_id(["group_id"], "value", vt=60)
await queue.delete_messages_by_ids([1, 2, 3])
```

### Message Type

```python
@dataclass(frozen=True)
class Message(Generic[T]):
    msg_id: int
    read_count: int
    enqueued_at: datetime
    vt: datetime
    message: T
```

## Group FIFO Pattern

The Group FIFO pattern allows parallel processing of different message groups while maintaining strict FIFO ordering within each group.

```python
# Send messages with group IDs
await pgmq.send_message("work", {"pr_id": "123", "action": "build"}, vt=0)
await pgmq.send_message("work", {"pr_id": "123", "action": "test"}, vt=0)
await pgmq.send_message("work", {"pr_id": "456", "action": "build"}, vt=0)

# Read by group - processes different pr_ids in parallel
msg = await pgmq.read_message_by_group_id("work", ["pr_id"], vt=60)
```

## License

MIT

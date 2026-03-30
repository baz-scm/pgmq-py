"""Pytest fixtures for pgmq-py tests."""

import os
import uuid
from collections.abc import AsyncGenerator

import pytest

from pgmq_py import PGMQ


@pytest.fixture
def database_url() -> str:
    """Get the database URL from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL environment variable not set")
    return url


@pytest.fixture
async def pgmq(database_url: str) -> AsyncGenerator[PGMQ, None]:
    """Create a PGMQ instance for testing."""
    async with PGMQ(database_url) as pgmq:
        await pgmq.create_schema()
        yield pgmq


@pytest.fixture
async def test_queue(pgmq: PGMQ) -> AsyncGenerator[str, None]:
    """Create a test queue with a unique name."""
    queue_name = f"test_{uuid.uuid4().hex[:8]}"
    await pgmq.create_queue(queue_name)
    yield queue_name
    await pgmq.delete_queue(queue_name)

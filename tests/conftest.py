import pytest

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.orm import sessionmaker

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
)

registry.register("memory", "sqlalchemy_memory.base", "MemoryDialect")
registry.register("memory.asyncio", "sqlalchemy_memory.async", "AsyncMemoryDialect")

@pytest.fixture
def SessionFactory():
    engine = create_engine("memory://", future=True)
    yield sessionmaker(engine, future=True)

@pytest.fixture
async def AsyncSessionFactory():
    engine = create_async_engine("memory+asyncio://")
    yield sessionmaker(engine, future=True, class_=AsyncSession)

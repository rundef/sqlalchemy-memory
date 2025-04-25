import pytest

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.orm import sessionmaker

from sqlalchemy.ext.asyncio import create_async_engine

from sqlalchemy_memory.base.session import MemorySession
from sqlalchemy_memory.asyncio.session import AsyncMemorySession

from models import Base

registry.register("memory", "sqlalchemy_memory.base", "MemoryDialect")
registry.register("memory.asyncio", "sqlalchemy_memory.asyncio", "AsyncMemoryDialect")

@pytest.fixture
def SessionFactory():
    engine = create_engine("memory://")
    Base.metadata.create_all(engine)

    yield sessionmaker(
        engine,
        class_=MemorySession,
        expire_on_commit=False,
    )

@pytest.fixture
async def AsyncSessionFactory():
    engine = create_async_engine("memory+asyncio://")

    conn = await engine.raw_connection() # force initialization of greenlet
    conn.close()

    Base.metadata.create_all(engine.sync_engine)

    yield sessionmaker(
        engine,
        class_=AsyncMemorySession,
        sync_session_class=MemorySession,
        future=True,
        expire_on_commit=False,
    )

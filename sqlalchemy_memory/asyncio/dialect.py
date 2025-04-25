from sqlalchemy.pool import AsyncAdaptedQueuePool

from ..base.dialect import MemoryDialect

class AsyncMemoryDialect(MemoryDialect):
    driver = "asyncio"
    supports_native_boolean = True
    supports_statement_cache = False
    is_async = True
    poolclass = AsyncAdaptedQueuePool

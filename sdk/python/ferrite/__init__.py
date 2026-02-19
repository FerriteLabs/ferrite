"""
ferrite-py -- Official Python client for Ferrite key-value store.

Ferrite is a high-performance tiered-storage key-value store designed as a
drop-in Redis replacement.  This package wraps ``redis-py`` and adds
first-class support for every Ferrite extension: vector search, semantic
caching, time series, document store, graph queries, FerriteQL, and
time-travel queries.

Quick start
-----------
Synchronous::

    from ferrite import FerriteClient

    client = FerriteClient(host="localhost", port=6379)
    client.set("key", "value")
    print(client.get("key"))
    client.close()

Asynchronous::

    from ferrite import AsyncFerriteClient
    import asyncio

    async def main():
        client = AsyncFerriteClient(host="localhost", port=6379)
        await client.set("key", "value")
        print(await client.get("key"))
        await client.close()

    asyncio.run(main())
"""

from ferrite.client import AsyncFerriteClient, FerriteClient
from ferrite.exceptions import (
    FerriteCommandError,
    FerriteConnectionError,
    FerriteError,
    FerriteSerializationError,
    FerriteTimeoutError,
)

__all__ = [
    "FerriteClient",
    "AsyncFerriteClient",
    "FerriteError",
    "FerriteConnectionError",
    "FerriteTimeoutError",
    "FerriteCommandError",
    "FerriteSerializationError",
]

__version__ = "0.1.0"

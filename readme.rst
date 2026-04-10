aiobreaker
==========

``aiobreaker`` is a fully asynchronous Python implementation of the
Circuit Breaker pattern, described in Michael T. Nygard's book
`Release It!`_.

Circuit breakers exist to allow one subsystem to fail without destroying
the entire system. This is done by wrapping dangerous operations
(typically integration points) with a component that can short-circuit
calls when the system is unhealthy.

This project is a fork of pybreaker_ by Daniel Fernandes Martins. From
**2.0** onward it is async-only and built on top of ``redis.asyncio``,
so the entire call/storage path is non-blocking.

.. _`Release It!`: https://pragprog.com/titles/mnee2/release-it-second-edition/
.. _pybreaker: https://github.com/danielfm/pybreaker

Features
--------

- Async-first ``CircuitBreaker`` with an in-memory or Redis state backend
- Async factory ``CircuitBreaker.create()`` so the breaker plays nicely
  with DI containers (constructors do **no** I/O)
- ``CircuitRedisStorage`` built on ``redis.asyncio.Redis`` with atomic
  ``set_opened_at`` via WATCH/MULTI for safe multi-process use
- Configurable failure threshold, reset timeout and excluded exceptions
- Pluggable ``CircuitBreakerListener`` for state change / call events
- Strict typing — passes ``mypy --strict``

Requirements
------------

- Python ``3.10`` or higher
- ``redis>=4.2`` (only when using the Redis backend)

Installation
------------

.. code:: bash

    pip install aiobreaker

Usage
-----

In-memory backend
~~~~~~~~~~~~~~~~~

The simplest setup uses the in-process memory storage:

.. code:: python

    from datetime import timedelta

    from aiobreaker import CircuitBreaker

    db_breaker = await CircuitBreaker.create(
        fail_max=5,
        timeout_duration=timedelta(seconds=60),
    )

    @db_breaker
    async def outside_integration():
        """Hit the API."""
        ...

    # via the decorator
    await outside_integration()

    # or imperatively
    await db_breaker.call_async(outside_integration)

Only async functions can be decorated — passing a synchronous function
raises ``TypeError``.

Redis backend
~~~~~~~~~~~~~

For sharing breaker state between processes, use ``CircuitRedisStorage``
together with a ``redis.asyncio.Redis`` client:

.. code:: python

    from datetime import timedelta

    from redis.asyncio import Redis

    from aiobreaker import (
        CircuitBreaker,
        CircuitBreakerState,
        CircuitRedisStorage,
    )

    redis_client = Redis(host="localhost", port=6379)

    storage = await CircuitRedisStorage.create(
        redis_object=redis_client,
        initial_state=CircuitBreakerState.CLOSED,
        namespace="S3",
    )

    breaker = await CircuitBreaker.create(
        fail_max=3,
        timeout_duration=timedelta(minutes=15),
        state_storage=storage,
    )

    @breaker
    async def fetch_from_s3():
        ...

    result = await breaker.call_async(fetch_from_s3)

``CircuitRedisStorage.create()`` seeds the Redis keys via ``SETNX`` so
existing state from other processes is preserved. If Redis becomes
unreachable, every read falls back to ``fallback_circuit_state`` (default
``CLOSED``) and the error is logged.

Inspecting state
~~~~~~~~~~~~~~~~

Because reading the breaker state can hit Redis, the inspection helpers
are coroutines:

.. code:: python

    state    = await breaker.get_current_state()  # CircuitBreakerState
    counter  = await breaker.get_fail_counter()   # int
    opens_at = await breaker.get_opens_at()       # datetime | None
    await breaker.sleep_until_open()              # async wait

Listeners
~~~~~~~~~

Plug into the breaker's lifecycle by subclassing
``CircuitBreakerListener``:

.. code:: python

    from aiobreaker import CircuitBreakerListener

    class Logger(CircuitBreakerListener):
        def state_change(self, breaker, old, new):
            print(f"{breaker.name}: {old.state if old else None} -> {new.state}")

    breaker = await CircuitBreaker.create(listeners=[Logger()])

Listener callbacks are synchronous on purpose — schedule
``asyncio.create_task(...)`` from inside if you need to do async work.

Migrating from 1.x
------------------

Version ``2.0`` is a breaking release. The synchronous code paths
(``CircuitBreaker.call``, generator support, sync ``CircuitRedisStorage``)
are gone, and storage is now async by design. To migrate:

- Replace ``CircuitBreaker(...)`` + immediate use with
  ``await CircuitBreaker.create(...)``.
- Replace ``CircuitRedisStorage(state, redis, ...)`` with
  ``await CircuitRedisStorage.create(redis_object=redis, initial_state=state, ...)``
  and pass a ``redis.asyncio.Redis`` (not the synchronous ``redis.Redis``).
- Use ``call_async`` instead of ``call``.
- Replace property access (``breaker.fail_counter``, ``breaker.current_state``,
  ``breaker.opens_at``) with the awaitable getters
  (``get_fail_counter``, ``get_current_state``, ``get_opens_at``).
- ``open()``, ``half_open()``, ``close()`` are now coroutines — ``await`` them.

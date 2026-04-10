import calendar
import logging
from datetime import datetime, timezone

from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from aiobreaker.state import CircuitBreakerState

from .base import CircuitBreakerStorage


class CircuitRedisStorage(CircuitBreakerStorage):
    """
    Implements a :class:`CircuitBreakerStorage` backed by an asynchronous
    ``redis.asyncio.Redis`` client.

    The constructor performs no I/O — required for usage in dependency
    injection containers where ``__init__`` cannot be a coroutine. Use
    :meth:`create` to obtain an instance with its keys initialized in Redis.
    """

    BASE_NAMESPACE = "aiobreaker"

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        redis_object: Redis,
        namespace: str | None = None,
        fallback_circuit_state: CircuitBreakerState = CircuitBreakerState.CLOSED,
    ) -> None:
        """
        Creates a new instance bound to the given async ``redis_object``.

        :param redis_object: A ``redis.asyncio.Redis`` instance.
        :param namespace: Optional namespace prepended to all keys.
        :param fallback_circuit_state: State returned when Redis is unreachable.
        """
        super().__init__("redis")
        self._redis: Redis = redis_object
        self._namespace_name: str | None = namespace
        self._fallback_circuit_state: CircuitBreakerState = fallback_circuit_state

    @classmethod
    async def create(
        cls,
        redis_object: Redis,
        initial_state: CircuitBreakerState = CircuitBreakerState.CLOSED,
        namespace: str | None = None,
        fallback_circuit_state: CircuitBreakerState = CircuitBreakerState.CLOSED,
    ) -> "CircuitRedisStorage":
        """
        Async factory: builds an instance and seeds the Redis keys via SETNX.

        Existing values are preserved — only missing keys are populated.
        """
        instance = cls(redis_object, namespace, fallback_circuit_state)
        await instance._initialize_redis_state(initial_state)
        return instance

    async def _initialize_redis_state(self, state: CircuitBreakerState) -> None:
        """
        Seeds the failure counter and state keys atomically using SETNX.
        """
        await self._redis.setnx(self._namespace("fail_counter"), 0)
        await self._redis.setnx(self._namespace("state"), state.name)

    async def get_state(self) -> CircuitBreakerState:
        """
        Returns the current circuit breaker state.

        If the state key is missing in Redis it is re-initialized with the
        configured fallback state. On any RedisError the fallback state is
        returned.
        """
        try:
            state_bytes = await self._redis.get(self._namespace("state"))
        except RedisError:
            self.logger.error(
                "RedisError: falling back to default circuit state",
                exc_info=True,
            )
            return self._fallback_circuit_state

        if state_bytes is not None:
            state_str = (
                state_bytes.decode("utf-8")
                if isinstance(state_bytes, (bytes, bytearray))
                else str(state_bytes)
            )
            try:
                return CircuitBreakerState[state_str]
            except KeyError:
                self.logger.error(
                    "Unknown circuit state %r in Redis; falling back",
                    state_str,
                )
                return self._fallback_circuit_state

        await self._initialize_redis_state(self._fallback_circuit_state)
        return self._fallback_circuit_state

    async def set_state(self, state: CircuitBreakerState) -> None:
        """
        Sets the current circuit breaker state.
        """
        try:
            await self._redis.set(self._namespace("state"), state.name)
        except RedisError:
            self.logger.error("RedisError", exc_info=True)

    async def increment_counter(self) -> None:
        """
        Increases the failure counter by one.
        """
        try:
            await self._redis.incr(self._namespace("fail_counter"))
        except RedisError:
            self.logger.error("RedisError", exc_info=True)

    async def reset_counter(self) -> None:
        """
        Sets the failure counter to zero.
        """
        try:
            await self._redis.set(self._namespace("fail_counter"), 0)
        except RedisError:
            self.logger.error("RedisError", exc_info=True)

    async def get_counter(self) -> int:
        """
        Returns the current value of the failure counter.
        """
        try:
            value = await self._redis.get(self._namespace("fail_counter"))
        except RedisError:
            self.logger.error("RedisError: assuming no errors", exc_info=True)
            return 0

        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            self.logger.error(
                "Invalid counter value %r in Redis; assuming 0", value
            )
            return 0

    async def get_opened_at(self) -> datetime | None:
        """
        Returns a timezone-aware UTC ``datetime`` of when the circuit was last
        opened, or ``None`` if it has never been opened.
        """
        try:
            timestamp = await self._redis.get(self._namespace("opened_at"))
        except RedisError:
            self.logger.error("RedisError", exc_info=True)
            return None

        if timestamp is None:
            return None
        try:
            return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
        except (TypeError, ValueError):
            self.logger.error(
                "Invalid opened_at value %r in Redis; treating as missing",
                timestamp,
            )
            return None

    async def set_opened_at(self, date_time: datetime) -> None:
        """
        Atomically updates the ``opened_at`` value, but only if the new value
        is strictly greater than the existing one (so that older "open" events
        cannot overwrite newer ones in a multi-process setup).

        ``date_time`` should be a UTC datetime; it is converted to a unix
        epoch integer for storage.
        """
        key = self._namespace("opened_at")
        next_value = int(calendar.timegm(date_time.utctimetuple()))

        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(key)
                        current = await pipe.get(key)
                        if current is None or next_value > int(current):
                            pipe.multi()  # type: ignore[no-untyped-call]
                            await pipe.set(key, next_value)
                            await pipe.execute()
                        else:
                            await pipe.unwatch()  # type: ignore[no-untyped-call]
                        break
                    except WatchError:
                        continue
        except RedisError:
            self.logger.error("RedisError", exc_info=True)

    def _namespace(self, key: str) -> str:
        name_parts = [self.BASE_NAMESPACE, key]
        if self._namespace_name:
            name_parts.insert(0, self._namespace_name)
        return ":".join(name_parts)

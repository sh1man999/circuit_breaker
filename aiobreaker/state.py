import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
)

if TYPE_CHECKING:
    from .circuitbreaker import CircuitBreaker


class CircuitBreakerError(Exception):
    """
    Raised when a guarded call fails because the breaker is open.
    """

    def __init__(self, message: str, reopen_time: datetime | None) -> None:
        """
        :param message: A short reason describing the failure.
        :param reopen_time: When the breaker will next allow a trial call.
        """
        super().__init__(message)
        self.message = message
        self.reopen_time = reopen_time

    @property
    def time_remaining(self) -> timedelta | None:
        if self.reopen_time is None:
            return None
        return self.reopen_time - datetime.now(timezone.utc)

    async def sleep_until_open(self) -> None:
        remaining = self.time_remaining
        if remaining is not None and remaining.total_seconds() > 0:
            await asyncio.sleep(remaining.total_seconds())


T = TypeVar("T")


class CircuitBreakerBaseState:
    """
    Base behavior shared by every circuit breaker state.

    Concrete subclasses (:class:`CircuitClosedState`, :class:`CircuitOpenState`,
    :class:`CircuitHalfOpenState`) override the ``on_enter`` / ``before_call`` /
    ``on_success`` / ``on_failure`` hooks. The class itself is intentionally
    not abstract — :meth:`call_async`, :meth:`_handle_error` and
    :meth:`_handle_success` are concrete implementations shared by every
    subclass.
    """

    def __init__(
        self, breaker: "CircuitBreaker", state: "CircuitBreakerState"
    ) -> None:
        self._breaker = breaker
        self._state = state

    @property
    def state(self) -> "CircuitBreakerState":
        """
        Returns the enum value identifying this state.
        """
        return self._state

    async def on_enter(
        self,
        prev_state: "CircuitBreakerBaseState | None" = None,
        notify: bool = False,
    ) -> None:
        """
        Async hook invoked after a state transition. Override in subclasses
        for side-effects (e.g. resetting counters or notifying listeners).
        """
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    async def _handle_error(
        self, func: Callable[..., Any] | None, exception: Exception
    ) -> None:
        """
        Handle a failed call. Always re-raises the original exception.
        """
        if self._breaker.is_system_error(exception):
            await self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exception)
            await self.on_failure(exception)
        else:
            await self._handle_success()
        raise exception

    async def _handle_success(self) -> None:
        """
        Handle a successful call: reset counter and notify listeners.
        """
        await self._breaker._state_storage.reset_counter()
        await self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    async def call_async(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Calls the async ``func`` according to the rules of the current state.
        """
        await self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = await func(*args, **kwargs)
        except Exception as exc:
            await self._handle_error(func, exc)
            raise  # unreachable; _handle_error always raises
        else:
            await self._handle_success()
            return ret

    async def before_call(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> None:
        """
        Hook invoked before each call. Default no-op.
        """
        return None

    async def on_success(self) -> None:
        """
        Hook invoked after a successful call. Default no-op.
        """
        return None

    async def on_failure(self, exception: Exception) -> None:
        """
        Hook invoked after a failed call. Default no-op.
        """
        return None


class CircuitClosedState(CircuitBreakerBaseState):
    """
    The "closed" state lets all calls through. Once the failure counter
    reaches the configured threshold the breaker transitions to the
    "open" state.
    """

    def __init__(self, breaker: "CircuitBreaker") -> None:
        super().__init__(breaker, CircuitBreakerState.CLOSED)

    async def on_enter(
        self,
        prev_state: "CircuitBreakerBaseState | None" = None,
        notify: bool = False,
    ) -> None:
        if notify:
            # Reset only when this is an explicit transition (not a snapshot
            # rebuild). This avoids clobbering a shared counter when several
            # CircuitBreaker instances watch the same Redis state.
            await self._breaker._state_storage.reset_counter()
        await super().on_enter(prev_state=prev_state, notify=notify)

    async def on_failure(self, exception: Exception) -> None:
        """
        Trip the breaker once the failure threshold is reached.
        """
        counter = await self._breaker._state_storage.get_counter()
        if counter >= self._breaker.fail_max:
            await self._breaker.open()
            opens_at = await self._breaker.get_opens_at()
            raise CircuitBreakerError(
                "Failures threshold reached, circuit breaker opened.",
                opens_at,
            ) from exception


class CircuitOpenState(CircuitBreakerBaseState):
    """
    The "open" state rejects calls immediately until the configured
    timeout has elapsed, after which the breaker becomes "half-open".
    """

    def __init__(self, breaker: "CircuitBreaker") -> None:
        super().__init__(breaker, CircuitBreakerState.OPEN)

    async def before_call(
        self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> None:
        timeout = self._breaker.timeout_duration
        opened_at = await self._breaker._state_storage.get_opened_at()
        if opened_at is not None and datetime.now(timezone.utc) < opened_at + timeout:
            opens_at = await self._breaker.get_opens_at()
            raise CircuitBreakerError(
                "Timeout not elapsed yet, circuit breaker still open",
                opens_at,
            )

    async def call_async(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        await self.before_call(func, *args, **kwargs)
        await self._breaker.half_open()
        return await self._breaker.call_async(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerBaseState):
    """
    The "half-open" state lets a single trial call through. On success the
    breaker closes again; on failure it returns to the open state.
    """

    def __init__(self, breaker: "CircuitBreaker") -> None:
        super().__init__(breaker, CircuitBreakerState.HALF_OPEN)

    async def on_failure(self, exception: Exception) -> None:
        await self._breaker.open()
        opens_at = await self._breaker.get_opens_at()
        raise CircuitBreakerError(
            "Trial call failed, circuit breaker opened.",
            opens_at,
        ) from exception

    async def on_success(self) -> None:
        await self._breaker.close()


class CircuitBreakerState(Enum):

    OPEN = CircuitOpenState
    CLOSED = CircuitClosedState
    HALF_OPEN = CircuitHalfOpenState

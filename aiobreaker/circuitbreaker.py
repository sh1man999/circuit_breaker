import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import (
    Any,
    TypeAlias,
    TypeVar,
)

from .listener import CircuitBreakerListener
from .state import (
    CircuitBreakerBaseState,
    CircuitBreakerError,
    CircuitBreakerState,
)
from .storage.base import CircuitBreakerStorage
from .storage.memory import CircuitMemoryStorage

T = TypeVar("T")

ExcludeItem: TypeAlias = type[BaseException] | Callable[[BaseException], bool]


class CircuitBreaker:
    """
    A circuit breaker is a route through which async functions are executed.
    When a function is executed via a circuit breaker, the breaker is notified.
    Multiple failed attempts will open the breaker and block additional calls
    until a configurable timeout has elapsed.

    Because the underlying storage is asynchronous, breakers must be created
    via :meth:`create` (or have ``_ensure_state_initialized`` awaited at least
    once before use). The constructor itself performs no I/O so the class is
    safe to instantiate from synchronous DI containers.
    """

    def __init__(
        self,
        fail_max: int = 5,
        timeout_duration: timedelta | None = None,
        exclude: Iterable[ExcludeItem] | None = None,
        listeners: Iterable[CircuitBreakerListener] | None = None,
        state_storage: CircuitBreakerStorage | None = None,
        name: str | None = None,
    ) -> None:
        """
        Creates a new circuit breaker with the given parameters.

        :param fail_max: Maximum number of failures before the breaker opens.
        :param timeout_duration: How long the breaker stays open before
            allowing a trial call.
        :param exclude: Exception types or predicates to ignore.
        :param listeners: Initial set of :class:`CircuitBreakerListener`.
        :param state_storage: A storage backend. Defaults to in-memory.
        :param name: Optional name for diagnostics/logging.
        """
        self._state_storage: CircuitBreakerStorage = (
            state_storage or CircuitMemoryStorage(CircuitBreakerState.CLOSED)
        )
        self._state: CircuitBreakerBaseState | None = None
        self._fail_max: int = fail_max
        self._timeout_duration: timedelta = (
            timeout_duration if timeout_duration is not None else timedelta(seconds=60)
        )
        self._excluded_exceptions: list[ExcludeItem] = list(exclude or [])
        self._listeners: list[CircuitBreakerListener] = list(listeners or [])
        self._name: str | None = name

    @classmethod
    async def create(
        cls,
        fail_max: int = 5,
        timeout_duration: timedelta | None = None,
        exclude: Iterable[ExcludeItem] | None = None,
        listeners: Iterable[CircuitBreakerListener] | None = None,
        state_storage: CircuitBreakerStorage | None = None,
        name: str | None = None,
    ) -> "CircuitBreaker":
        """
        Async factory: builds the breaker and primes its cached state from
        the storage backend.
        """
        breaker = cls(
            fail_max=fail_max,
            timeout_duration=timeout_duration,
            exclude=exclude,
            listeners=listeners,
            state_storage=state_storage,
            name=name,
        )
        await breaker._ensure_state_initialized()
        return breaker

    async def _ensure_state_initialized(self) -> None:
        """
        Lazily builds the cached state object on first use.
        """
        if self._state is None:
            current = await self._state_storage.get_state()
            self._state = self._build_state(current)

    def _build_state(
        self, new_state: CircuitBreakerState
    ) -> CircuitBreakerBaseState:
        """
        Construct (without invoking ``on_enter``) a state object for the
        given enum value.
        """
        state_cls = new_state.value
        instance: CircuitBreakerBaseState = state_cls(self)
        return instance

    async def _transition_to(
        self, new_state: CircuitBreakerState, notify: bool = True
    ) -> CircuitBreakerBaseState:
        """
        Replace the cached state with a freshly built object for ``new_state``
        and run its async ``on_enter`` hook.
        """
        prev_state = self._state
        next_state = self._build_state(new_state)
        self._state = next_state
        await next_state.on_enter(prev_state=prev_state, notify=notify)
        return next_state

    async def get_state(self) -> CircuitBreakerBaseState:
        """
        Returns the current cached state object, refreshing it from storage
        if it has drifted (e.g. another process changed the shared state).
        """
        await self._ensure_state_initialized()
        assert self._state is not None
        current = await self._state_storage.get_state()
        if current != self._state.state:
            await self._transition_to(current, notify=True)
        assert self._state is not None
        return self._state

    async def get_current_state(self) -> CircuitBreakerState:
        """
        Returns the current circuit breaker state enum from the storage.
        """
        return await self._state_storage.get_state()

    async def get_fail_counter(self) -> int:
        """
        Returns the current number of consecutive failures.
        """
        return await self._state_storage.get_counter()

    async def get_opens_at(self) -> datetime | None:
        """
        Returns the UTC datetime at which the breaker will allow another
        trial call, or ``None`` if it has already elapsed (or never opened).
        """
        opened_at = await self._state_storage.get_opened_at()
        if opened_at is None:
            return None
        opens_at = opened_at + self._timeout_duration
        if opens_at < datetime.now(timezone.utc):
            return None
        return opens_at

    async def get_time_until_open(self) -> timedelta | None:
        """
        Returns the remaining time until the breaker will retry, or
        ``None`` if it has already elapsed.
        """
        opens_at = await self.get_opens_at()
        if opens_at is None:
            return None
        remaining = opens_at - datetime.now(timezone.utc)
        if remaining < timedelta(0):
            return None
        return remaining

    async def sleep_until_open(self) -> None:
        """
        Sleeps asynchronously until the breaker will accept another call.
        No-op if there is no remaining timeout.
        """
        remaining = await self.get_time_until_open()
        if remaining is not None and remaining.total_seconds() > 0:
            await asyncio.sleep(remaining.total_seconds())

    @property
    def fail_max(self) -> int:
        """
        Returns the maximum number of failures tolerated before the circuit
        is opened.
        """
        return self._fail_max

    @fail_max.setter
    def fail_max(self, number: int) -> None:
        self._fail_max = number

    @property
    def timeout_duration(self) -> timedelta:
        """
        Returns how long the breaker stays open before allowing a trial call.
        """
        return self._timeout_duration

    @timeout_duration.setter
    def timeout_duration(self, timeout: timedelta) -> None:
        self._timeout_duration = timeout

    @property
    def excluded_exceptions(self) -> tuple[ExcludeItem, ...]:
        """
        Returns a tuple of the excluded exception types/predicates.
        """
        return tuple(self._excluded_exceptions)

    def add_excluded_exception(self, exception: ExcludeItem) -> None:
        self._excluded_exceptions.append(exception)

    def add_excluded_exceptions(self, *exceptions: ExcludeItem) -> None:
        for exc in exceptions:
            self.add_excluded_exception(exc)

    def remove_excluded_exception(self, exception: ExcludeItem) -> None:
        self._excluded_exceptions.remove(exception)

    async def _inc_counter(self) -> None:
        """
        Increments the failure counter in storage.
        """
        await self._state_storage.increment_counter()

    def is_system_error(self, exception: BaseException) -> bool:
        """
        Returns whether ``exception`` should count as a system error and
        therefore trip the breaker. Excluded exception types and predicates
        return ``False``.
        """
        exception_type = type(exception)
        for exclusion in self._excluded_exceptions:
            if isinstance(exclusion, type):
                if issubclass(exception_type, exclusion):
                    return False
            elif callable(exclusion):
                if exclusion(exception):
                    return False
        return True

    async def call_async(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Calls the async ``func`` according to the rules of the current state.
        """
        if getattr(func, "_ignore_on_call", False):
            return await func(*args, **kwargs)

        state = await self.get_state()
        return await state.call_async(func, *args, **kwargs)

    async def open(self) -> None:
        """
        Opens the circuit. Subsequent calls fail fast until the timeout
        elapses.
        """
        await self._state_storage.set_opened_at(datetime.now(timezone.utc))
        await self._state_storage.set_state(CircuitBreakerState.OPEN)
        await self._transition_to(CircuitBreakerState.OPEN, notify=True)

    async def half_open(self) -> None:
        """
        Half-opens the circuit, letting the next call act as a trial.
        """
        await self._state_storage.set_state(CircuitBreakerState.HALF_OPEN)
        await self._transition_to(CircuitBreakerState.HALF_OPEN, notify=True)

    async def close(self) -> None:
        """
        Closes the circuit, returning to normal operation.
        """
        await self._state_storage.set_state(CircuitBreakerState.CLOSED)
        await self._transition_to(CircuitBreakerState.CLOSED, notify=True)

    def __call__(
        self,
        *call_args: Any,
        ignore_on_call: bool = True,
    ) -> Callable[..., Any]:
        """
        Decorates an async function so its calls are routed through this
        breaker. Synchronous functions are rejected with ``TypeError`` —
        this library is async-only.

        :param ignore_on_call: When ``True`` the wrapped function will not
            re-trigger the breaker if it is invoked via :meth:`call_async`.
        """

        def _outer_wrapper(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            if not inspect.iscoroutinefunction(func):
                raise TypeError(
                    "CircuitBreaker can only decorate async functions; "
                    f"{getattr(func, '__name__', func)!r} is synchronous."
                )

            @wraps(func)
            async def _inner_wrapper_async(*args: Any, **kwargs: Any) -> T:
                return await self.call_async(func, *args, **kwargs)

            _inner_wrapper_async._ignore_on_call = ignore_on_call  # type: ignore[attr-defined]
            return _inner_wrapper_async

        if len(call_args) == 1 and (
            inspect.isfunction(call_args[0]) or inspect.ismethod(call_args[0])
        ):
            return _outer_wrapper(call_args[0])
        if len(call_args) == 0:
            return _outer_wrapper
        raise TypeError("Decorator does not accept positional arguments.")

    @property
    def listeners(self) -> tuple[CircuitBreakerListener, ...]:
        return tuple(self._listeners)

    def add_listener(self, listener: CircuitBreakerListener) -> None:
        self._listeners.append(listener)

    def add_listeners(self, *listeners: CircuitBreakerListener) -> None:
        for listener in listeners:
            self.add_listener(listener)

    def remove_listener(self, listener: CircuitBreakerListener) -> None:
        self._listeners.remove(listener)

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, name: str | None) -> None:
        self._name = name


__all__ = ("CircuitBreaker", "CircuitBreakerError")

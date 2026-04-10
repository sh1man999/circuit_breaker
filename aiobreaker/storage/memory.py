from datetime import datetime

from aiobreaker.state import CircuitBreakerState

from .base import CircuitBreakerStorage


class CircuitMemoryStorage(CircuitBreakerStorage):
    """
    Implements a :class:`CircuitBreakerStorage` in local process memory.

    The constructor performs no I/O and only seeds in-process attributes,
    so it is safe to instantiate from synchronous contexts.
    """

    def __init__(self, state: CircuitBreakerState) -> None:
        """
        Creates a new instance with the given initial `state`.
        """
        super().__init__("memory")
        self._fail_counter: int = 0
        self._opened_at: datetime | None = None
        self._state: CircuitBreakerState = state

    async def get_state(self) -> CircuitBreakerState:
        """
        Returns the current circuit breaker state.
        """
        return self._state

    async def set_state(self, state: CircuitBreakerState) -> None:
        """
        Sets the current circuit breaker state.
        """
        self._state = state

    async def increment_counter(self) -> None:
        """
        Increases the failure counter by one.
        """
        self._fail_counter += 1

    async def reset_counter(self) -> None:
        """
        Sets the failure counter to zero.
        """
        self._fail_counter = 0

    async def get_counter(self) -> int:
        """
        Returns the current value of the failure counter.
        """
        return self._fail_counter

    async def get_opened_at(self) -> datetime | None:
        """
        Returns the most recent value of when the circuit was opened.
        """
        return self._opened_at

    async def set_opened_at(self, date_time: datetime) -> None:
        """
        Sets the most recent value of when the circuit was opened.
        """
        self._opened_at = date_time

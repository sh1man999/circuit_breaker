from abc import ABC, abstractmethod
from datetime import datetime

from aiobreaker.state import CircuitBreakerState


class CircuitBreakerStorage(ABC):
    """
    Defines the underlying storage for a circuit breaker. Concrete subclasses
    must implement every abstract method declared below.

    All state-mutating and state-reading methods are coroutines so that they
    can be used uniformly with both in-memory and remote backends (e.g. Redis).
    """

    def __init__(self, name: str) -> None:
        """
        Creates a new instance identified by `name`.
        """
        self._name = name

    @property
    def name(self) -> str:
        """
        Returns a human friendly name that identifies this storage backend.
        """
        return self._name

    @abstractmethod
    async def get_state(self) -> CircuitBreakerState:
        """
        Returns the current circuit breaker state.
        """

    @abstractmethod
    async def set_state(self, state: CircuitBreakerState) -> None:
        """
        Sets the current circuit breaker state.
        """

    @abstractmethod
    async def increment_counter(self) -> None:
        """
        Increases the failure counter by one.
        """

    @abstractmethod
    async def reset_counter(self) -> None:
        """
        Sets the failure counter to zero.
        """

    @abstractmethod
    async def get_counter(self) -> int:
        """
        Returns the current value of the failure counter.
        """

    @abstractmethod
    async def get_opened_at(self) -> datetime | None:
        """
        Returns the most recent value of when the circuit was opened, or
        ``None`` if the circuit has never been opened.
        """

    @abstractmethod
    async def set_opened_at(self, date_time: datetime) -> None:
        """
        Sets the most recent value of when the circuit was opened.
        """

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .circuitbreaker import CircuitBreaker
    from .state import CircuitBreakerBaseState


class CircuitBreakerListener:
    """
    Listener class used to plug code into a :class:`CircuitBreaker` instance
    so that user-supplied callbacks fire when certain events happen.

    Listener methods are synchronous by design — they should not perform
    blocking I/O. If you need async work, schedule a task from inside the
    listener (e.g. via :func:`asyncio.create_task`).
    """

    def before_call(
        self,
        breaker: "CircuitBreaker",
        func: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Called before a function is executed via the breaker.
        """

    def failure(
        self, breaker: "CircuitBreaker", exception: BaseException
    ) -> None:
        """
        Called when a function executed over the breaker fails.
        """

    def success(self, breaker: "CircuitBreaker") -> None:
        """
        Called when a function executed over the breaker succeeds.
        """

    def state_change(
        self,
        breaker: "CircuitBreaker",
        old: Optional["CircuitBreakerBaseState"],
        new: "CircuitBreakerBaseState",
    ) -> None:
        """
        Called when the state of the breaker changes.
        """

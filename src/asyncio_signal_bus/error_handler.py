from logging import getLogger
from typing import Awaitable, Callable, Generic

from asyncio_signal_bus.types import R, S

LOGGER = getLogger(__name__)


class SubscriberErrorHandler(Generic[S, R]):
    """
    Error handler for subscribers. This class may be subclassed if more complex error
    handling is needed. However, generally, all errors should terminate here.
    """

    def __init__(self, f: Callable[[S], Awaitable[R]]):
        self._f = f

    def __call__(self, signal: S) -> R:
        try:
            return self._f(signal)
        except Exception as e:
            LOGGER.exception(f"An error occurred in a subscriber. {repr(e)}")

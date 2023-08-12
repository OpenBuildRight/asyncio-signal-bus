from logging import getLogger
from typing import Any, Awaitable, Callable

LOGGER = getLogger(__name__)


class SubscriberErrorHandler:
    def __init__(self, callable: Callable[[Any], Awaitable]):
        self._callable = callable

    def __call__(self, signal: Any):
        try:
            return self._callable(signal)
        except Exception as e:
            LOGGER.exception(f"An error occurred in a subscriber. {repr(e)}")

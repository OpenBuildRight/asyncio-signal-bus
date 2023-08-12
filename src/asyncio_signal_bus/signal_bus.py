import asyncio
from asyncio.queues import Queue
from collections.abc import Callable
from logging import getLogger
from typing import Any, Awaitable, Dict, List, TypeVar

from asyncio_signal_bus.error_handler import SubscriberErrorHandler
from asyncio_signal_bus.publisher import SignalPublisher
from asyncio_signal_bus.subscriber import SignalSubscriber

LOGGER = getLogger(__name__)

S = TypeVar("S")
R = TypeVar("R")


class SignalBus:
    def __init__(self):
        self._queues: Dict[str, Queue] = {}
        self._subscribers: List[SignalSubscriber] = []
        self._publishers = []

    def get_queue(self, queue_name: str):
        return self._queues.get(queue_name)

    def subscriber(self, topic_name="default", error_handler=SubscriberErrorHandler):
        self._queues.setdefault(topic_name, Queue())
        queue = self._queues.get(topic_name)

        def _wrapper(f: Callable[[Any], Awaitable[Any]]):
            s = SignalSubscriber(error_handler(f), queue)
            LOGGER.debug(f"Registering subscriber to topic {topic_name}")
            self._subscribers.append(s)
            return s

        return _wrapper

    def publisher(self, topic_name="default"):
        self._queues.setdefault(topic_name, Queue())
        queue = self._queues.get(topic_name)

        def _wrapper(f: Callable[[Any], Awaitable[Any]]):
            return SignalPublisher(f, queue)

        return _wrapper

    async def __aenter__(self):
        await asyncio.gather(*[x.start() for x in self._subscribers])

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.gather(*[x.start() for x in self._subscribers])

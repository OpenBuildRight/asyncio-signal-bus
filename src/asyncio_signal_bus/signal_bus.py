import asyncio
from collections.abc import Callable
from typing import Dict, Generic
from asyncio.queues import Queue
from typing import Any, Awaitable, TypeVar, Generic, List
from logging import getLogger


LOGGER = getLogger(__name__)

S = TypeVar("S")
R = TypeVar("R")

class SignalSubscriber(Generic[S, R]):
    def __init__(self, callable: Callable[[S], Awaitable[R]], queue: Queue):
        self._callable = callable
        self._queue = queue
        self._listening_task: asyncio.Task = None

    async def listen(self):
        LOGGER.debug("Started listening.")
        while True:
            signal = self._queue.get()
            asyncio.create_task(self(signal))
        LOGGER.debug("No longer listening.")

    async def start(self):
        if self._listening_task is None:
            self._listening_task = asyncio.create_task(self.listen())
    async def stop(self):
        self._listening_task.cancel()

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def __call__(self, signal: S) -> R:
        return await self._callable(signal)

class SignalPublisher:
    def __init__(self, callable: Callable[..., Awaitable[R]], queue: Queue):
        self._callable = callable
        self._queue = queue

    async def __call__(self, *args, **kwargs):
        result = await self._callable(*args, **kwargs)
        await self._queue.put(result)
        return result

class SignalBus:
    def __init__(self):
        self._queues: Dict[str, Queue] = {}
        self._subscribers: List[SignalSubscriber] = []
        self._publishers = []

    def get_queue(self, queue_name: str):
        return self._queues.get(queue_name)

    def subscriber(self, topic_name="default"):
        self._queues.setdefault(topic_name, Queue())
        queue = self._queues.get(topic_name)
        def _wrapper(f: Callable[[Any], Awaitable[Any]]):
            s = SignalSubscriber(
               f,
               queue
            )
            self._subscribers.append(s)
            return s
        return _wrapper

    def publisher(self, topic_name="default"):
        self._queues.setdefault(topic_name, Queue())
        queue = self._queues.get(topic_name)
        def _wrapper(f: Callable[[Any], Awaitable[Any]]):
            return SignalPublisher(
                f,
                queue
            )
        return _wrapper

    async def __aenter__(self):
        await asyncio.gather(*[x.start() for x in self._subscribers])

    async def __aexit__(self):
        await asyncio.gather(*[x.start() for x in self._subscribers])

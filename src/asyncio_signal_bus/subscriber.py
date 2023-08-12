import asyncio
from asyncio import Queue
from logging import getLogger
from typing import Awaitable, Callable, Generic, TypeVar

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
            signal = await self._queue.get()
            asyncio.create_task(self(signal))
        LOGGER.debug("No longer listening.")

    async def start(self):
        LOGGER.info("Starting signal subscriber")
        if self._listening_task is None:
            self._listening_task = asyncio.create_task(self.listen())

    async def stop(self):
        self._listening_task.cancel()
        LOGGER.info("Signal subscriber stopped.")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def __call__(self, signal: S) -> R:
        return await self._callable(signal)

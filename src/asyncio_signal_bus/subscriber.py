import asyncio
from asyncio import Queue, Task
from logging import getLogger
from typing import Awaitable, Callable, Generic, TypeVar

LOGGER = getLogger(__name__)
S = TypeVar("S")
R = TypeVar("R")


class SignalSubscriber(Generic[S, R]):
    def __init__(self, f: Callable[[S], Awaitable[R]], queue: Queue):
        self._f = f
        self._queue = queue
        self._listening_task: Task = None

    async def _task_wrapper(self, coroutine):
        await coroutine
        self._queue.task_done()

    async def listen(self):
        LOGGER.debug("Started listening.")
        while True:
            signal = await self._queue.get()
            asyncio.create_task(self._task_wrapper(self(signal)))

    async def start(self):
        LOGGER.info("Starting signal subscriber")
        if self._listening_task is None:
            self._listening_task = asyncio.create_task(self.listen())

    async def stop(self):
        await self._queue.join()
        canceled = False
        LOGGER.debug("All tasks have completed.")
        while not canceled:
            canceled = self._listening_task.cancel()
        LOGGER.info("Signal subscriber stopped.")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def __call__(self, signal: S) -> R:
        return await self._f(signal)

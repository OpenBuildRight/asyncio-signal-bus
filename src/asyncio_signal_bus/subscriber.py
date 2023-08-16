import asyncio
from asyncio import Queue, Task
from logging import getLogger
from time import time
from typing import Awaitable, Callable, Generic, Optional, SupportsFloat, SupportsInt

from asyncio_signal_bus.exception import SignalBusShutdownError
from asyncio_signal_bus.types import R, S

LOGGER = getLogger(__name__)


class SignalSubscriber(Generic[S, R]):
    """
    Wrapper class used to subscribe an async callable to an asyncio queue.
    """

    def __init__(
        self,
        f: Callable[[S], Awaitable[R]],
        queue: Queue,
        shutdown_timeout: SupportsFloat = 120,
    ):
        self._f = f
        self._queue: Queue = queue
        self._listening_task: Optional[Task] = None
        self.shutdown_timeout: float = float(shutdown_timeout)

    async def _task_wrapper(self, coroutine):
        await coroutine
        self._queue.task_done()

    async def _listen(self):
        LOGGER.debug("Started listening.")
        while True:
            signal = await self._queue.get()
            asyncio.create_task(self._task_wrapper(self(signal)))

    async def start(self):
        """
        If it is not possible to use this class as a context manager, it may be started
        and stopped manually. Just make sure that you call the method during shutdown.
        """
        LOGGER.debug("Starting signal subscriber")
        if self._listening_task is None:
            self._listening_task = asyncio.create_task(self._listen())

    async def stop(self):
        """
        Stop the subscriber.
        """
        try:
            await asyncio.wait_for(self._queue.join(), timeout=self.shutdown_timeout)
        except asyncio.exceptions.TimeoutError as e:
            raise SignalBusShutdownError(
                f"Unable to shutdown tasks in the queue within timeout of"
                f"{self.shutdown_timeout} seconds. "
                f"The timeout can be adjusted with the shutdown timeout parameter if "
                f"you need the task to have more time. {repr(e)}"
            )
        LOGGER.debug("All tasks have completed.")
        canceled = False
        while not canceled:
            canceled = self._listening_task.cancel()
        LOGGER.debug("Signal subscriber stopped.")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def __call__(self, signal: S, *args, **kwargs) -> R:
        return await self._f(signal, *args, **kwargs)


class BatchSignalSubscriber(SignalSubscriber):
    def __init__(
        self,
        f: Callable[[S], Awaitable[R]],
        queue: Queue,
        shutdown_timeout: SupportsFloat = 120,
        max_items: SupportsInt = 10,
        period_seconds: SupportsFloat = 10,
    ):
        super().__init__(f, queue, shutdown_timeout)
        self.max_items = int(max_items)
        self.period_seconds = float(period_seconds)

    async def _batch_task_wrapper(self, coroutine, n_items: int):
        await coroutine
        for i in range(n_items):
            self._queue.task_done()

    async def _listen(self):
        LOGGER.debug("Started listening.")
        batch = []
        ts = time()
        while True:
            if len(batch) < self.max_items and not self._queue.empty():
                signal = self._queue.get_nowait()
                batch.append(signal)
            if batch and (
                len(batch) >= self.max_items or (time() - ts) > self.period_seconds
            ):
                asyncio.create_task(self._batch_task_wrapper(self(batch), len(batch)))
                ts = time()
                batch = []
            # This deadlocks unless there is some sleep. It does not need to be long.
            # My guess is that under the hood we just need to allow at least one clock
            # cycle for something else to happen so that the event loop can perform
            # other operations.
            await asyncio.sleep(1e-10)

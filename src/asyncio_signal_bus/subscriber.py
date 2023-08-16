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
        back_off_time: SupportsFloat = 0.0001,
    ):
        self._f = f
        self._queue: Queue = queue
        self._in_context = False
        self._listening_task: Optional[Task] = None
        self.shutdown_timeout: float = float(shutdown_timeout)
        self.back_off_time: float = float(back_off_time)

    async def _task_wrapper(self, coroutine):
        await coroutine
        self._queue.task_done()

    async def _listen(self):
        LOGGER.debug("Started listening.")
        while self._in_context or not self._queue.empty():
            signal = await self._queue.get()
            asyncio.create_task(self._task_wrapper(self(signal)))
            if self._queue.empty():
                await asyncio.sleep(self.back_off_time)
        LOGGER.debug("Stopped listening.")

    async def start(self):
        """
        If it is not possible to use this class as a context manager, it may be started
        and stopped manually. Just make sure that you call the method during shutdown.
        """
        LOGGER.debug("Starting signal subscriber")
        self._in_context = True
        if self._listening_task is None:
            self._listening_task = asyncio.create_task(self._listen())

    async def stop(self):
        """
        Stop the subscriber.
        """
        self._in_context = False
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

        while not self._listening_task.done() and not self._listening_task.cancelled():
            self._listening_task.cancel()
            await asyncio.sleep(self.shutdown_timeout * 0.001)
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
        back_off_time: SupportsFloat = None,
        max_items: SupportsInt = 10,
        period_seconds: SupportsFloat = 10,
    ):
        if period_seconds > shutdown_timeout:
            raise ValueError("Period seconds must be less than the shutdown timeout.")
        if back_off_time and back_off_time > period_seconds:
            raise ValueError("Backoff time should be less than period_seconds.")
        if back_off_time is None:
            back_off_time = float(period_seconds) * 0.25
        super().__init__(f, queue, shutdown_timeout, back_off_time)
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
        while self._in_context or not self._queue.empty() or batch:
            if len(batch) < self.max_items and not self._queue.empty():
                signal = self._queue.get_nowait()
                batch.append(signal)
            if batch and (
                len(batch) >= self.max_items or (time() - ts) > self.period_seconds
            ):
                asyncio.create_task(self._batch_task_wrapper(self(batch), len(batch)))
                ts = time()
                batch = []
            if not batch:
                await asyncio.sleep(self.back_off_time)
            await asyncio.sleep(1e-10)
        LOGGER.debug("Subscriber stopped.")

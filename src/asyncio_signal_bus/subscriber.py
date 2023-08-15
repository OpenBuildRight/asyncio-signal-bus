import asyncio
from asyncio import Queue, Task
from logging import getLogger
from typing import Awaitable, Callable, Generic, Optional, SupportsFloat

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

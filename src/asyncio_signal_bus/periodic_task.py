import asyncio
from typing import Callable, Any
from asyncio.locks import Lock
from logging import getLogger

LOGGER = getLogger(__name__)


class PeriodicTask:

    def __init__(
            self,
            f: Callable[[], Any],
            period_seconds: int = 10
    ):
        self._f = f
        self._started = False
        self._lock = Lock()
        self.period_seconds = period_seconds

    async def periodic_task(self):
        while self._started:
            async with self._lock:
                await self._f()
            await asyncio.sleep(self.period_seconds)

    async def start(self):
        self._started = True
        LOGGER.debug("Starting periodic task.")
        asyncio.create_task(self.periodic_task())

    async def stop(self):
        self._started = False
        async with self._lock.acquire():
            LOGGER.debug("Periodic task stopped.")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

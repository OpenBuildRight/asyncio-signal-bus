import asyncio
from asyncio import Queue
from typing import Awaitable, Callable, Generic, List

from asyncio_signal_bus.types import R


class SignalPublisher(Generic[R]):
    def __init__(self, f: Callable[..., Awaitable[R]], queues: List[Queue]):
        self._f = f
        self._queues = queues

    async def __call__(self, *args, **kwargs) -> R:
        result = await self._f(*args, **kwargs)
        await asyncio.gather(*[q.put(result) for q in self._queues])
        return result

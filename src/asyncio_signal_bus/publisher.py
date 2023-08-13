import asyncio
from asyncio import Queue
from typing import Awaitable, Callable, List, TypeVar

R = TypeVar("R")


class SignalPublisher:
    def __init__(self, f: Callable[..., Awaitable[R]], queues: List[Queue]):
        self._f = f
        self._queues = queues

    async def __call__(self, *args, **kwargs):
        result = await self._f(*args, **kwargs)
        await asyncio.gather(*[q.put(result) for q in self._queues])
        return result

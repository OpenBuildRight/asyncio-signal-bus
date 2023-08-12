from asyncio import Queue
from typing import Awaitable, Callable, TypeVar

R = TypeVar("R")


class SignalPublisher:
    def __init__(self, callable: Callable[..., Awaitable[R]], queue: Queue):
        self._callable = callable
        self._queue = queue

    async def __call__(self, *args, **kwargs):
        result = await self._callable(*args, **kwargs)
        await self._queue.put(result)
        return result

import asyncio
from typing import Awaitable, Callable, Generic

from asyncio_signal_bus.queue_getter import QueueGetter
from asyncio_signal_bus.types import R


class SignalPublisher(Generic[R]):
    def __init__(self, f: Callable[..., Awaitable[R]], queue_getter: QueueGetter):
        self._f = f
        self._queue_getter = queue_getter

    async def __call__(self, *args, **kwargs) -> R:
        queues = await self._queue_getter()
        result = await self._f(*args, **kwargs)
        await asyncio.gather(*[q.put(result) for q in queues])
        return result

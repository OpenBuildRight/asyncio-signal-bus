from asyncio import Queue

import pytest

from asyncio_signal_bus.publisher import SignalPublisher
from asyncio_signal_bus.queue_getter import QueueGetter


@pytest.mark.asyncio
async def test_signal_publisher():
    async def foo_publisher(arg: str):
        return arg.upper()

    queues = {}
    queue_getter = QueueGetter("foo", queues)

    publisher = SignalPublisher(foo_publisher, queue_getter)
    queues["foo"] = [Queue(), Queue(), Queue()]

    await publisher("a")
    for queue in queues["foo"]:
        assert await queue.get() == "A"

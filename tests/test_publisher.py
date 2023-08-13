from asyncio import Queue

import pytest

from asyncio_signal_bus.publisher import SignalPublisher


@pytest.mark.asyncio
async def test_signal_publisher():
    async def foo_publisher(arg: str):
        return arg.upper()

    queues = [Queue(), Queue(), Queue()]

    publisher = SignalPublisher(foo_publisher, queues)

    await publisher("a")
    for queue in queues:
        assert await queue.get() == "A"

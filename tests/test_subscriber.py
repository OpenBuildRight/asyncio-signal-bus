import asyncio
from asyncio import Queue
from unittest.mock import Mock

import pytest

from asyncio_signal_bus.subscriber import SignalSubscriber
from asyncio_signal_bus.exception import SignalBusShutdownError


@pytest.mark.asyncio
async def test_subscriber():
    subscriber_queue = Queue()

    target_mock = Mock()

    async def foo_subscriber(signal: str):
        target_mock(signal)

    signal_subscriber = SignalSubscriber(foo_subscriber, subscriber_queue)
    async with signal_subscriber:
        await subscriber_queue.put("a")
    target_mock.assert_called_once_with("a")


@pytest.mark.asyncio
async def test_subscriber_timeout():
    subscriber_queue = Queue()


    async def foo_subscriber(signal: str):
        await asyncio.sleep(1)

    signal_subscriber = SignalSubscriber(foo_subscriber, subscriber_queue, shutdown_timeout=0.01)
    with pytest.raises(SignalBusShutdownError):
        async with signal_subscriber:
            await subscriber_queue.put("a")

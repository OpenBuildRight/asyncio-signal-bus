from asyncio import Queue
from unittest.mock import Mock

import pytest

from asyncio_signal_bus.subscriber import SignalSubscriber


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

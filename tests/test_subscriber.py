import asyncio
from asyncio import Queue
from typing import List
from unittest.mock import Mock, call

import pytest

from asyncio_signal_bus.batch_counter import LengthBatchCounter
from asyncio_signal_bus.exception import SignalBusShutdownError
from asyncio_signal_bus.subscriber import BatchSignalSubscriber, SignalSubscriber


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

    signal_subscriber = SignalSubscriber(
        foo_subscriber, subscriber_queue, shutdown_timeout=0.01
    )
    with pytest.raises(SignalBusShutdownError):
        async with signal_subscriber:
            await subscriber_queue.put("a")


@pytest.mark.asyncio
async def test_batch_subscriber():
    subscriber_queue = Queue()

    target_mock = Mock()

    async def foo_subscriber(signal: List[int]):
        target_mock(signal)

    signal_subscriber = BatchSignalSubscriber(
        foo_subscriber,
        subscriber_queue,
        max_items=3,
        period_seconds=0.1,
        shutdown_timeout=0.3,
    )
    async with signal_subscriber:
        for i in range(5):
            await subscriber_queue.put(i)
        await asyncio.sleep(0.2)
        await subscriber_queue.put(6)
    target_mock.assert_has_calls([call([0, 1, 2]), call([3, 4]), call([6])])

@pytest.mark.asyncio
async def test_batch_subscriber_length():
    subscriber_queue = Queue()

    target_mock = Mock()

    async def foo_subscriber(signal: List[int]):
        target_mock(signal)

    signal_subscriber = BatchSignalSubscriber(
        foo_subscriber,
        subscriber_queue,
        max_items=100,
        period_seconds=0.1,
        shutdown_timeout=0.3,
        batch_counters=[LengthBatchCounter(max_length=9)],
    )
    async with signal_subscriber:
        await subscriber_queue.put("abc")
        await subscriber_queue.put("de")
        await subscriber_queue.put("fghij")
        await subscriber_queue.put("klmnop")
        await subscriber_queue.put("qrs")
    target_mock.assert_has_calls([call("abc", "de"), call(["fghij"]), call(["klmnop", "qrs"])], any_order=True)

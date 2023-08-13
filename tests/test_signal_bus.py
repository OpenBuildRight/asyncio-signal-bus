import asyncio
import logging
import sys
from asyncio.queues import Queue
from logging import StreamHandler

import pytest

from asyncio_signal_bus import SignalBus


@pytest.mark.asyncio
async def test_round_trip():
    bus = SignalBus()
    result_queue = Queue()

    @bus.publisher(topic_name="foo")
    async def foo_publisher(arg: str):
        print("Publishing message.")
        await asyncio.sleep(1)
        return f"message:{arg}"

    @bus.subscriber(topic_name="foo")
    async def foo_subscriber(signal: str):
        print("Received message.")
        await asyncio.sleep(1)
        await result_queue.put(signal)

    input = ["a", "b", "c"]
    expected_output = ["message:a", "message:b", "message:c"]

    async with bus:
        await asyncio.gather(*[foo_publisher(x) for x in input])
    results = []
    while not result_queue.empty():
        results.append(await result_queue.get())
    results.sort()
    assert expected_output == results


@pytest.mark.asyncio
async def test_round_trip_with_classes():
    bus = SignalBus()
    result_queue = Queue()

    class FooPublisher:
        @bus.publisher(topic_name="foo")
        async def foo_publisher(arg: str):
            print("Publishing message.")
            await asyncio.sleep(1)
            return f"message:{arg}"

    class FooSubscriber:
        @bus.subscriber(topic_name="foo")
        async def foo_subscriber(signal: str):
            print("Received message.")
            await asyncio.sleep(1)
            await result_queue.put(signal)

    input = ["a", "b", "c"]
    expected_output = ["message:a", "message:b", "message:c"]

    foo_publisher = FooPublisher()
    FooSubscriber()

    async with bus:
        await asyncio.gather(*[foo_publisher.foo_publisher(x) for x in input])
    results = []
    while not result_queue.empty():
        results.append(await result_queue.get())
    results.sort()
    assert expected_output == results


@pytest.mark.asyncio
async def test_chaining():
    logging.getLogger().setLevel("DEBUG")
    handler = StreamHandler(sys.stderr)
    logging.getLogger().addHandler(handler)
    handler.setLevel("DEBUG")
    bus = SignalBus()
    result_queue = Queue()

    @bus.publisher(topic_name="bar")
    async def bar_publisher(arg: str):
        print("Publishing message.")
        await asyncio.sleep(1)
        return f"message:{arg}"

    @bus.subscriber(topic_name="bar")
    @bus.publisher(topic_name="foo")
    async def bar_subscriber(signal: str):
        return signal

    @bus.subscriber(topic_name="foo")
    async def foo_subscriber(signal: str):
        print("Received message.")
        await asyncio.sleep(1)
        await result_queue.put(signal)

    input = ["a", "b", "c"]
    expected_output = ["message:a", "message:b", "message:c"]

    async with bus:
        await asyncio.gather(*[bar_publisher(x) for x in input])
    results = []
    while not result_queue.empty():
        results.append(await result_queue.get())
    results.sort()
    assert expected_output == results

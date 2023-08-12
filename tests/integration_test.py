import asyncio
import logging
from logging import getLogger, StreamHandler
import sys

import pytest

from asyncio_signal_bus import SignalBus
from asyncio.queues import Queue


@pytest.mark.asyncio
async def test_round_trip():
    logging.getLogger().setLevel("DEBUG")
    handler = StreamHandler(sys.stderr)
    logging.getLogger().addHandler(handler)
    handler.setLevel("DEBUG")
    bus = SignalBus()
    result_queue = Queue()
    @bus.publisher(topic_name="foo")
    async def foo_publisher(arg: str):
        print("Publishing message.")
        return f"message:{arg}"

    @bus.subscriber(topic_name="foo")
    async def foo_subscriber(signal: str):
        print("Received message.")
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


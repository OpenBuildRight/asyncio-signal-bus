import asyncio
from asyncio.queues import Queue

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
async def test_round_trip_in_same_class():
    result_queue = Queue()

    class FooPublisher:
        bus = SignalBus()

        def __init__(self):
            self.foo_subscriber = self.bus.subscriber(topic_name="foo")(
                self.foo_subscriber
            )
            self.foo_publisher = self.bus.publisher(topic_name="foo")(
                self.foo_publisher
            )

        async def foo_publisher(self, arg: str):
            print("Publishing message.")
            await asyncio.sleep(0.01)
            return f"message:{arg}"

        async def foo_subscriber(self, signal: str):
            print("Received message.")
            await asyncio.sleep(0.01)
            await result_queue.put(signal)

    input = ["a", "b", "c"]
    expected_output = ["message:a", "message:b", "message:c"]

    foo_publisher = FooPublisher()

    async with foo_publisher.bus:
        await asyncio.gather(*[foo_publisher.foo_publisher(x) for x in input])
    results = []
    while not result_queue.empty():
        results.append(await result_queue.get())
    results.sort()
    assert expected_output == results


@pytest.mark.asyncio
async def test_chaining():
    bus = SignalBus()
    result_queue = Queue()

    @bus.publisher(topic_name="bar")
    async def bar_publisher(arg: str):
        print("Publishing message.")
        await asyncio.sleep(0.2)
        return f"message:{arg}"

    @bus.subscriber(topic_name="bar")
    @bus.publisher(topic_name="foo")
    async def bar_subscriber(signal: str):
        return signal

    @bus.subscriber(topic_name="foo")
    async def foo_subscriber(signal: str):
        print("Received message.")
        await asyncio.sleep(0.2)
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


@pytest.mark.asyncio
async def test_batch_subscriber():
    bus = SignalBus()
    result_queue = Queue()

    @bus.publisher(topic_name="foo")
    async def foo_publisher(arg: str):
        print("Publishing message.")
        return f"message:{arg}"

    @bus.batch_subscriber(
        topic_name="foo",
        shutdown_timeout=10,
        period_seconds=1,
        max_items=3,
        back_off_time=0.001,
    )
    async def foo_subscriber(signal: str):
        print("Received message.")
        await result_queue.put(signal)

    expected_output = [["message:0", "message:1", "message:2"], ["message:3"]]

    async with bus:
        for x in range(4):
            await foo_publisher(x)
    results = []
    while not result_queue.empty():
        results.append(await result_queue.get())
    results.sort()
    assert results == expected_output


@pytest.mark.asyncio
async def test_function_wrapper_subscriber():
    BUS = SignalBus()

    @BUS.subscriber()
    def foo(signal: str): ...

    assert hasattr(foo, "__name__")


@pytest.mark.asyncio
async def test_function_wrapper_batch_subscriber():
    BUS = SignalBus()

    @BUS.batch_subscriber()
    def foo(signal: str): ...

    assert hasattr(foo, "__name__")


@pytest.mark.asyncio
async def test_function_wrapper_publisher():
    BUS = SignalBus()

    @BUS.publisher()
    def foo(signal: str): ...

    assert hasattr(foo, "__name__")


@pytest.mark.asyncio
async def test_function_wrapper_inject():
    BUS = SignalBus()

    async def bar_factory():
        return "bar"

    @BUS.inject("bar", bar_factory)
    async def foo(bar: str):
        return bar.upper()

    assert hasattr(foo, "__name__")


@pytest.mark.asyncio
async def test_periodic_task():
    BUS = SignalBus()

    result = []

    @BUS.periodic_task(period_seconds=0.1)
    async def add():
        result.append(1)

    async with BUS:
        await asyncio.sleep(0.35)

    assert result == [1, 1, 1, 1]


@pytest.mark.asyncio
async def test_periodic_task_gracefull_exit():
    BUS = SignalBus()

    result = []

    @BUS.periodic_task(period_seconds=0.1)
    async def add():
        await asyncio.sleep(0.2)
        result.append(1)

    async with BUS:
        await asyncio.sleep(0.01)

    assert result == [1]

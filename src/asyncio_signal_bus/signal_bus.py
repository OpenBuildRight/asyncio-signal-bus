import asyncio
import functools
from asyncio.queues import Queue
from logging import getLogger
from typing import Awaitable, Callable, Dict, List, Optional, SupportsFloat, Type, Tuple, Sequence

from asyncio_signal_bus.batch_counter import BatchCounterAbc
from asyncio_signal_bus.error_handler import SubscriberErrorHandler
from asyncio_signal_bus.injector import Injector
from asyncio_signal_bus.periodic_task import PeriodicTask
from asyncio_signal_bus.publisher import SignalPublisher
from asyncio_signal_bus.queue_getter import QueueGetter
from asyncio_signal_bus.subscriber import BatchSignalSubscriber, SignalSubscriber
from asyncio_signal_bus.types import R, S

LOGGER = getLogger(__name__)


class SignalBus:
    """
    Asyncio signal bus which uses asyncio queues to send messages between publishers
    and subscribers. The signal bus should be used as a context manager in order to
    start subscribers listening to messages.

    For example, we can declare a signal bus and a couple methods as a publisher a
    subscriber.

    >>> BUS = SignalBus()

    >>> @BUS.publisher(topic_name="greeting")
    ... async def generate_uppercase(arg: str):
    ...     return arg.upper()

    We can declare as many subscribers or publishers as we want for a single topic. We
    just have to make sure that they are able to handle the same data type for the
    signal.

    >>> @BUS.subscriber(topic_name="greeting")
    ... async def print_signal(signal: str):
    ...     await asyncio.sleep(0.1)
    ...     print(signal)

    >>> @BUS.subscriber(topic_name="greeting")
    ... async def reply(signal: str):
    ...     await asyncio.sleep(0.2)
    ...     print("HELLO FROM WORLD!")

    We then call the publish method within async context. As long as we remain within
    context, the subscribers will receive the signal.

    >>> async def main():
    ...     async with BUS:
    ...         await generate_uppercase("hello world!")


    >>> asyncio.run(main())
    HELLO WORLD!
    HELLO FROM WORLD!
    """

    def __init__(self, injector=None):
        self._queues: Dict[str, List[Queue]] = {}
        self._subscribers: List[SignalSubscriber] = []
        self.injector = injector if injector else Injector()
        self._periodic_tasks = []

    def get_queue(self, queue_name: str) -> List[Queue]:
        return self._queues.get(queue_name)

    def subscriber(
        self,
        topic_name="default",
        error_handler: Type[SubscriberErrorHandler] = SubscriberErrorHandler[S, R],
        shutdown_timeout: Optional[SupportsFloat] = 120,
        back_off_time: SupportsFloat = 0.1,
    ) -> Callable[[Callable[[S], Awaitable[R]]], SignalSubscriber[S, R]]:
        """
        Decorator for asyncio methods subscribing to a topic. The method must take a
        single argument, which is the signal it will receive from a publisher. The
        signal may be any data type.
        :param topic_name: The name of the topic used to link one or more subscribers
            with one or more publishers.
        :param error_handler: An error handler used to handle errors within the
            callable. Error handling should usually terminate at the subscriber, with
            the subscriber catching all exceptions. Any unhandled errors will block the
            shutdown of the bus when the bus exits context or the stop method is used.
        :param shutdown_timeout: If the subscriber takes longer than this time during
            shutdown, then the task is killed and an error is raised. If you do not
            want the task timeout to be limited, then set this value to None.
        :param back_off_time: If the queue is empty, wait this long before
            getting another item.
        :return: Wrapped callable.
        """
        self._queues.setdefault(topic_name, [])
        queue = Queue()
        self._queues.get(topic_name).append(queue)

        def _wrapper(f):
            s = SignalSubscriber(
                error_handler(f),
                queue,
                shutdown_timeout=shutdown_timeout,
                back_off_time=back_off_time,
            )
            LOGGER.debug(f"Registering subscriber to topic {topic_name}")
            self._subscribers.append(s)

            @functools.wraps(f)
            def inner_wrapper(*args, **kwargs):
                return s(*args, **kwargs)

            return inner_wrapper

        return _wrapper

    def batch_subscriber(
        self,
        topic_name="default",
        error_handler: Type[SubscriberErrorHandler] = SubscriberErrorHandler[S, R],
        shutdown_timeout: Optional[SupportsFloat] = 120,
        back_off_time: SupportsFloat = None,
        max_items: int = 10,
        period_seconds: float = 10,
        batch_counters: Sequence[BatchCounterAbc] = tuple()
    ):
        """
        A subscriber that consumes batches of events. The subscriber will wait no longer
        than the period_seconds between aggregations. Batches will not exceed batch
        seconds in size.

        >>> import json
        >>> BUS = SignalBus()

        >>> @BUS.publisher(topic_name="greeting")
        ... async def generate_uppercase(arg: str):
        ...     return arg.upper()

        >>> @BUS.batch_subscriber(
        ...     topic_name="greeting",
        ...     max_items = 3,
        ...     period_seconds = 1,
        ... )
        ... async def print_batch(batch: List[str]):
        ...     print(json.dumps(batch))

        >>> async def main():
        ...     async with BUS:
        ...         await asyncio.gather(*[generate_uppercase(f"hello {x}") for x in range(9)])
        >>> asyncio.run(main())
        ["HELLO 0", "HELLO 1", "HELLO 2"]
        ["HELLO 3", "HELLO 4", "HELLO 5"]
        ["HELLO 6", "HELLO 7", "HELLO 8"]

        Additional counters can be added to further limit batches. For example, you
        can use the LengthBatchCounter to control the total length of items sent.
        >>> from asyncio_signal_bus.batch_counter import LengthBatchCounter
        >>> BUS = SignalBus()

        >>> @BUS.publisher(topic_name="greeting")
        ... async def generate_uppercase(arg: str):
        ...     return arg.upper()

        >>> @BUS.batch_subscriber(
        ...     topic_name="greeting",
        ...     max_items = 3,
        ...     period_seconds = 1,
        ...     batch_counters=[LengthBatchCounter(9)]
        ... )
        ... async def print_batch(batch: List[str]):
        ...     all_items = "".join(batch)
        ...     print(f"items = {all_items}, length = {len(all_items)}")

        >>> async def main():
        ...     async with BUS:
        ...         await generate_uppercase("abc")
        ...         await generate_uppercase("defg")
        ...         await generate_uppercase("h")
        ...         await generate_uppercase("ijklmnopqrs")
        ...         await generate_uppercase("tu")
        ...         await generate_uppercase("vwxyz")
        >>> asyncio.run(main())
        items = "abcdefgh" length = 9

        :param topic_name: The name of the topic used to link one or more subscribers
            with one or more publishers.
        :param error_handler: An error handler used to handle errors within the
            callable. Error handling should usually terminate at the subscriber, with
            the subscriber catching all exceptions. Any unhandled errors will block the
            shutdown of the bus when the bus exits context or the stop method is used.
        :param shutdown_timeout: If the subscriber takes longer than this time during
            shutdown, then the task is killed and an error is raised. If you do not
            want the task timeout to be limited, then set this value to None.
        :param back_off_time: If the queue is empty, wait this long before
            getting another item. If back off time is None, then a default value is
            calculated from the period_seconds.
        :param max_items: The maximum amount of items for the batch.
        :param period_seconds: The maximum amount of time to wait between batches.
        :param batch_counters: Sequence of custom batch counters used to limit the
            size of a batch. If any batch counter is full, including the batch counters
            created from max_items and period_seconds, then the batch is sent.
        :return: Wrapped callable
        """
        self._queues.setdefault(topic_name, [])
        queue = Queue()
        self._queues.get(topic_name).append(queue)

        def _wrapper(f):
            s = BatchSignalSubscriber(
                error_handler(f),
                queue,
                shutdown_timeout=shutdown_timeout,
                max_items=max_items,
                period_seconds=period_seconds,
                back_off_time=back_off_time,
                batch_counters=batch_counters,
            )
            LOGGER.debug(f"Registering subscriber to topic {topic_name}")
            self._subscribers.append(s)

            @functools.wraps(f)
            def inner_wrapper(*args, **kwargs):
                return s(*args, **kwargs)

            return inner_wrapper

        return _wrapper

    def publisher(self, topic_name="default"):
        """
        Decorator for asyncio methods. The publisher returns a signal which is passed
        to subscribers subscribed to the same topic name. The signal may be any data
        type.
        :param topic_name: The name of the topic used to link one or more subscribers
            with one or more publishers.
        :return: wrapped callable
        """
        queue_getter = QueueGetter(topic_name, self._queues)

        def _wrapper(f):
            publisher = SignalPublisher(f, queue_getter)

            @functools.wraps(f)
            def inner_wrapper(*args, **kwargs):
                return publisher(*args, **kwargs)

            return inner_wrapper

        return _wrapper

    def inject(self, arg_name: str, factory: Callable[..., Awaitable]):
        """
        Decorator used to inject the argument arg_name using the return value of the
        coroutine factory if the SignalBus is within context. Inject uses an
        instance of the Inject class which tracks the same context as the
        SignalBus for convenience. Injectors are usually needed in subscribers which
        need additional data, such as URL's, secrets and usernames and connection pools
        not received in the signal.

        >>> BUS = SignalBus()

        >>> @BUS.publisher(topic_name="greeting")
        ... async def generate_greeting(arg: str):
        ...     return arg

        >>> async def name_factory():
        ...     return "Frank"

        >>> @BUS.subscriber(topic_name="greeting")
        ... @BUS.inject("name", name_factory)
        ... async def print_greeting(greeting: str, name: str):
        ...     print(f"{greeting} from {name}")
        >>> async def main():
        ...     async with BUS:
        ...         await generate_greeting("hello")
        >>> asyncio.run(main())
        hello from Frank

        Note that, while the injector is wrapped in the bus for convenience, you do not
        need to use the bus to use the injector. The Injector is a stand-alone class
        that can be used without the SignalBus. In addition, you can decorate things
        other than publishers and subscribers. The Injector can take the place of the
        web framework's dependency injector so that the same injector system can be
        used accross multiple protocols.

        :param arg_name: THe name of the argument to inject into the method.
        :param factory:
        :return:
        """
        return self.injector.inject(arg_name, factory)

    def periodic_task(self, period_seconds: int = 10):
        """
        Create a periodic task.

        >>> BUS = SignalBus()
        >>> @BUS.periodic_task(period_seconds=0.5)
        ... async def print_foos():
        ...     print("foo")
        >>> async def main():
        ...     async with BUS:
        ...         await asyncio.sleep(2)
        >>> asyncio.run(main())
        foo
        foo
        foo
        foo

        The task will block exiting the bus context until it has completed.

        :param period_seconds:
        :return: Wrapped callable
        """

        def wrapper(f):
            periodic_task = PeriodicTask(f, period_seconds=period_seconds)
            self._periodic_tasks.append(periodic_task)

            @functools.wraps(f)
            def _inner_wrapper(*args, **kwargs):
                return periodic_task(*args, **kwargs)

            return _inner_wrapper

        return wrapper

    async def start(self):
        """
        Start the signal bus. This is used in cases where it is not practical
        to use this class as an asyncio context manager. Starting the bus starts the
        subscribers listening to the queue. Any signals sent prior to start will be
        processed as soon as the bus is started. You must remember to stop the bus
        during shutdown yourself.
        """
        LOGGER.debug("Starting bus.")
        await asyncio.gather(
            self.injector.start(),
            *[x.start() for x in self._subscribers + self._periodic_tasks],
        )
        LOGGER.debug("Bus started.")

    async def stop(self):
        """
        Stop the bus. This method is used in cases where it is not practical to use the
        class as an asyncio context manager. Stopping the bus stops all subscribers
        from receiving new signals. The stop method will block until all subscribers
        complete. The error handler is expected to catch all errors so that all
        subscribers are guaranteed to complete.
        :return:
        """
        LOGGER.debug("Stopping bus.")
        await asyncio.gather(
            self.injector.stop(),
            *[x.stop() for x in self._subscribers + self._periodic_tasks],
        )
        LOGGER.debug("Bus stopped.")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

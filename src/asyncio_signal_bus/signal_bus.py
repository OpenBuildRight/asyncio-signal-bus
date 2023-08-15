import asyncio
from asyncio.queues import Queue
from logging import getLogger
from typing import Awaitable, Callable, Dict, List, Optional, SupportsFloat, Type

from asyncio_signal_bus.error_handler import SubscriberErrorHandler
from asyncio_signal_bus.injector import Injector
from asyncio_signal_bus.publisher import SignalPublisher
from asyncio_signal_bus.subscriber import SignalSubscriber
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

    def get_queue(self, queue_name: str) -> List[Queue]:
        return self._queues.get(queue_name)

    def subscriber(
        self,
        topic_name="default",
        error_handler: Type[SubscriberErrorHandler] = SubscriberErrorHandler[S, R],
        shutdown_timeout: Optional[SupportsFloat] = 120,
    ) -> Callable[[Callable[[S], Awaitable[R]]], SignalSubscriber[S, R]]:
        """
        Decorator for asyncio methods subscribing to a topic. The method must take a
        single argument, which is the signal it will receive from a publisher. The
        signal may be any data type.
        :param topic_name: The name of the topic used to link one or more subscribers
            with one or more publishers.
        :param error_handler: An error handler used to handle errors within the callable.
            Error handling should usually terminate at the subscriber, with the
            subscriber catching all exceptions. Any unhandled errors will block the
            shutdown of the bus when the bus exits context or the stop method is used.
        :param shutdown_timeout: If the subscriber takes longer than this time during
            shutdown, then the task is killed and an error is raised. If you do not
            want the task timeout to be limited, then set this value to None.
        :return: Wrapped callable.
        """
        self._queues.setdefault(topic_name, [])
        queue = Queue()
        self._queues.get(topic_name).append(queue)

        def _wrapper(f: Callable[[S], Awaitable[R]]) -> SignalSubscriber[S, R]:
            s = SignalSubscriber(error_handler(f), queue)
            LOGGER.debug(f"Registering subscriber to topic {topic_name}")
            self._subscribers.append(s)
            return s

        return _wrapper

    def publisher(
        self, topic_name="default"
    ) -> Callable[[Callable[..., Awaitable[S]]], SignalPublisher[S]]:
        """
        Decorator for asyncio methods. The publisher returns a signal which is passed
        to subscribers subscribed to the same topic name. The signal may be any data
        type.
        :param topic_name: The name of the topic used to link one or more subscribers
            with one or more publishers.
        :return: wrapped callable
        """
        self._queues.setdefault(topic_name, [])
        queues = self._queues.get(topic_name)

        def _wrapper(f: Callable[..., Awaitable[S]]) -> SignalPublisher[S]:
            return SignalPublisher(f, queues)

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
            self.injector.start(), *[x.start() for x in self._subscribers]
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
            self.injector.stop(), *[x.stop() for x in self._subscribers]
        )
        LOGGER.debug("Bus stopped.")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

import abc
import asyncio
import functools
from asyncio.locks import Lock
from typing import Awaitable, Callable, Generic, TypeVar

R = TypeVar("R")


class InjectorCallable(Generic[R]):
    def __init__(
        self,
        f: Callable[..., Awaitable[R]],
        arg_name: str,
        factory: Callable[..., Awaitable],
    ):
        self._in_context = False
        self._in_context_lock = Lock()
        self._f = f
        self.arg_name = arg_name
        self.factory = factory

    async def start(self):
        async with self._in_context_lock:
            self._in_context = True

    async def stop(self):
        async with self._in_context_lock:
            self._in_context = False

    async def __call__(self, *args, **kwargs) -> R:
        _kwargs = {}
        if self._in_context and kwargs.get(self.arg_name) is None:
            _kwargs[self.arg_name] = await self.factory()
        _kwargs.update(kwargs)
        return await self._f(*args, **_kwargs)


class InjectorAbc(abc.ABC):

    @property
    @abc.abstractmethod
    def injector_callables(self) -> set[InjectorCallable]: ...


class Injector(InjectorAbc):
    """
    Class for dependency injection. Factory methods may be injected at runtime as
    long as the injector is within context. During unit testing or other times that
    you don't want dependency injection, simply leave the Injector out of context.
    >>> import json
    >>> async def foo_factory():
    ...     return "FOO"

    >>> async def bar_factory():
    ...     return "BAR"
    >>> INJECTOR = Injector()

    >>> @INJECTOR.inject("foo", foo_factory)
    ... @INJECTOR.inject("bar", bar_factory)
    ... async def foobar(foo=None, bar=None):
    ...     return {"foo": foo, "bar": bar}
    >>> async def main():
    ...     print("Without context:")
    ...     print(json.dumps(await foobar()))
    ...     async with INJECTOR:
    ...         print("With context:")
    ...         print(json.dumps(await foobar()))

    >>> asyncio.run(main())
    Without context:
    {"foo": null, "bar": null}
    With context:
    {"foo": "FOO", "bar": "BAR"}
    """

    def __init__(self):
        self._injector_callables: set[InjectorCallable] = set()
        self._injector_callables_lock = Lock()

    @property
    def injector_callables(self) -> set[InjectorCallable]:
        return self._injector_callables

    def connect(self, *injectors: InjectorAbc):
        """
        Connect injectors.
        :param injectors:
        :return:
        """
        for injector in injectors:
            self._injector_callables.update(injector.injector_callables)
        for injector in injectors:
            injector.injector_callables.update(self.injector_callables)

    def inject(self, arg_name: str, factory: Callable[..., Awaitable]):
        def _inject(f: Callable[..., Awaitable]):
            injector_callable = InjectorCallable(f, arg_name, factory)
            self._injector_callables.add(injector_callable)

            @functools.wraps(f)
            def _wrapper(*args, **kwargs):
                return injector_callable(*args, **kwargs)

            return _wrapper

        return _inject

    async def start(self):
        async with self._injector_callables_lock:
            await asyncio.gather(*[c.start() for c in self._injector_callables])

    async def stop(self):
        async with self._injector_callables_lock:
            await asyncio.gather(*[c.stop() for c in self._injector_callables])

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

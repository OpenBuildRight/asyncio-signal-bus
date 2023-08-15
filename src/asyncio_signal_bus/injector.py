import asyncio
from asyncio.locks import Lock
from typing import Awaitable, Callable, Generic, List, TypeVar

R = TypeVar("R")


class Injector:
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
        self._injector_callables: List[InjectorCallable] = []
        self._injector_callables_lock = Lock()

    def inject(self, arg_name: str, factory: Callable[..., Awaitable]):
        def _inject(f: Callable[..., Awaitable]):
            injector_callable = InjectorCallable(f, arg_name, factory)
            self._injector_callables.append(injector_callable)
            return injector_callable

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

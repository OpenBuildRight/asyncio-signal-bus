import pytest

from asyncio_signal_bus.injector import Injector

@pytest.mark.asyncio
async def test_injector():

    INJECTOR = Injector()

    async def foo_factory():
        return "FOO"

    async def bar_factory():
        return "BAR"
    @INJECTOR.inject("foo", foo_factory)
    @INJECTOR.inject("bar", bar_factory)
    async def foobar(foo=None, bar=None):
        return {"foo": foo, "bar": bar}

    assert {"foo": None, "bar": None} == await foobar()

    async with INJECTOR:
        assert {"foo": "FOO", "bar": "BAR"} == await foobar()


@pytest.mark.asyncio
async def test_nested_injector():

    INJECTOR = Injector()

    async def foo_factory():
        return "FOO"

    @INJECTOR.inject("foo", foo_factory)
    async def foo_dict_factory(foo=None):
        return {"foo": foo}

    @INJECTOR.inject("foo_dict", foo_dict_factory)
    async def foobar(foo_dict=None):
        ret = {} if foo_dict is None else foo_dict
        ret["bar"] = "BAR"
        return ret


    assert {"bar": "BAR"} == await foobar()

    async with INJECTOR:
        assert {"foo": "FOO", "bar": "BAR"} == await foobar()

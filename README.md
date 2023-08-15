# Asyncio Signal Bus

Asyncio Signal Bus is a simple utility for sending publisher subscriber 
signals from one python function to another, thus allowing you to simplify asynchronous 
applications using event based architecture.

## Installation
Install with pip.

```shell
pip install asyncio-signal-bus
```

## Usage
Simply decorate your publishers and subscribers to pass any data type asynchronously 
whenever the publisher returns a value. 

```python
import asyncio

from asyncio_signal_bus import SignalBus
import json
from typing import Dict

BUS = SignalBus()

@BUS.publisher(topic_name="foo")
async def foo_publisher(arg: str):
    signal = {"message": arg}
    print(f"publishing {json.dumps(signal)}")
    await asyncio.sleep(1)
    return signal

@BUS.subscriber(topic_name="foo")
async def foo_subscriber_0(signal: Dict):
    print(f"foo subscriber 0 received {json.dumps(signal)}")

@BUS.subscriber(topic_name="foo")
async def foo_subscriber_1(signal: Dict):
    print(f"foo subscriber 1 received {json.dumps(signal)}")
```

In order to start the subscribers listening, you use the bus context. When the bus 
leaves context, all subscribers stop listening.
```python
async def main():
    inputs = [f"message:{i}" for i in range(10)]
    async with BUS:
        await asyncio.gather(*[foo_publisher(x) for x in inputs])
```
That is it! You can string together publishers and subscribers in whatever way you want.
You can also annotate functions and methods used for other purposes as publishers in 
order to facilitate post processing and reporting hooks without having to pass them 
to the function.

## Dependency Injection
Subscribers often need additional data beyond the signal, such as URL's, secrets, 
usernames and connection pools. The inject decorator can be used to inject additional 
data to other arguments as kwargs defaults. Injection only occors when the bus or it's 
injector is within context.
```python
import asyncio

from asyncio_signal_bus import SignalBus
BUS = SignalBus()

@BUS.publisher(topic_name="greeting")
async def generate_greeting(arg: str):
    return arg

async def name_factory():
     return "Frank"

@BUS.subscriber(topic_name="greeting")
@BUS.inject("name", name_factory)
async def print_greeting(greeting: str, name: str):
    print(f"{greeting} from {name}")
async def main():
    async with BUS:
        await generate_greeting("hello")
asyncio.run(main())
```

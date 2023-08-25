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
## Periodic Tasks
Sometimes we need to run tasks periodically such as polling an external database or an 
internal queue. We also want to make sure that when our application shuts down, it 
gracefully tells the periodic tasks to stop starting new tasks but it waits for the 
existing task to complete.
```python
import asyncio
from asyncio_signal_bus import SignalBus

BUS = SignalBus()
@BUS.periodic_task(period_seconds=0.5)
async def print_foos():
     print("foo")
     
async def main():
     async with BUS:
         await asyncio.sleep(2)
asyncio.run(main())
```
### Period Tasks As Publishers
Given that the periodic task is the furthest upstream in a process, we may use it to
drive downstream subscribers. We can combine the periodic task with the injectors and 
the publisher to periodically fetch data using configuration information and send it 
to subscribers for processing.

```python
import asyncio
from typing import List, Dict
from asyncio_signal_bus import SignalBus
import os

BUS = SignalBus()

async def get_url():
    return os.environ.get("URL")

async def get_secret():
    return os.environ.get("SECRET")

@BUS.periodic_task(period_seconds=1)
@BUS.publisher(topic_name="new-data")
@BUS.inject("url", get_url)
@BUS.inject("secret", get_secret)
async def get_data(url: str, secret: str):
    # Perform some sort of IO here to get your data.
    return [
        {"id": 0, "value": "cats"},
        {"id": 1, "value": "dogs"}
    ]

@BUS.subscriber(topic_name="new-data")
async def process_values(data: List[Dict]):
    for row in data:
        print(row["value"].upper())

@BUS.subscriber(topic_name="new-data")
async def process_ids(data: List[Dict]):
    for row in data:
        print(row["id"] + 1)

async def main():
    async with BUS:
        await asyncio.sleep(5)
asyncio.run(main())
```
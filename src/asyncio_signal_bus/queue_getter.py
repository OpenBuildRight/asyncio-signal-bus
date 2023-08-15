from asyncio import Queue
from typing import Dict, Tuple


class QueueGetter:

    def __init__(self, queue_name, queues: Dict):
        self.queue_name = queue_name
        self.queues = queues

    async def __call__(self)-> Tuple[Queue]:
        return self.queues.get(self.queue_name, tuple())

from abc import ABC, abstractmethod
from time import time
from typing import Callable, SupportsFloat, Optional

from asyncio_signal_bus.types import S


class BatchCounterAbc(ABC):

    @abstractmethod
    def put(self, value: S) -> None: ...

    @abstractmethod
    def is_full(self) -> bool: ...

    @abstractmethod
    def would_fill(self, value: S) -> bool: ...

    @abstractmethod
    def clear(self) -> None: ...


class CountBatchCounter(BatchCounterAbc):

    def __init__(self, max_items: int):
        self._count = 0
        self._max_items = max_items

    def put(self, value: S) -> None:
        self._count += 1

    def is_full(self) -> bool:
        return self._count >= self._max_items

    def would_fill(self, value: S) -> bool:
        return (self._count + 1) >= self._max_items

    def clear(self) -> None:
        self._count = 0


class TimeBatchCounter(BatchCounterAbc):

    def __init__(
            self,
            max_period_seconds: float,
            time_provider: Callable[[], SupportsFloat] = time
    ):
        self._max_period_seconds = max_period_seconds
        self._time_provider = time_provider
        self._time_start: Optional[SupportsFloat] = None

    def put(self, value: S):
        if self._time_start is None:
            self._time_start = self._time_provider()

    def is_full(self) -> bool:
        if self._time_start is None:
            self._time_start = self._time_provider()
        time_now: SupportsFloat = self._time_provider()
        time_elapsed = time_now - self._time_start
        return time_elapsed >= self._max_period_seconds

    def would_fill(self, value: S) -> bool:
        return self.is_full()

    def clear(self):
        self._time_start = self._time_provider()


class LengthBatchCounter(BatchCounterAbc):
    def __init__(self, max_length: int):
        self._max_length = max_length
        self._count = 0

    def put(self, value: S):
        try:
            self._count += len(value)
        except TypeError as e:
            raise TypeError(f"Class {type(self)} requires values which support len()") from e

    def is_full(self) -> bool:
        return self._count >= self._max_length

    def would_fill(self, value: S) -> bool:
        return (self._count + len(value)) >= self._max_length

    def clear(self) -> None:
        self._count = 0

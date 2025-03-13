import time

from asyncio_signal_bus.batch_counter import CountBatchCounter, TimeBatchCounter

def test_count_batch_counter_is_full():
    counter = CountBatchCounter(max_items=15)
    assert not counter.is_full()
    for i in range(14):
        counter.put("foo")
        assert not counter.is_full()
    counter.put("foo")
    assert counter.is_full()
    counter.clear()
    assert not counter.is_full()

def test_time_batch_counter_is_full():
    counter = TimeBatchCounter(max_period_seconds=0.5)
    assert not counter.is_full()
    time.sleep(0.6)
    assert counter.is_full()
    counter.clear()
    assert not counter.is_full()

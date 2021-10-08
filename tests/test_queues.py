from asyncio import sleep
from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase

from pyrill.queues import Queue
from pyrill.sources import AsyncSource


class QueueTestCase(IsolatedAsyncioTestCase):

    async def test_success_preload(self):
        async def gen():
            for i in range(10):
                yield i
                await sleep(0.1)

        queue = AsyncSource[int](source=gen()) >> Queue()
        queue.start_consumer()

        await sleep(1)
        ts = datetime.now()

        async for i in queue:
            ts_2 = datetime.now()
            self.assertIsInstance(i, int)
            self.assertLess(ts_2 - ts, timedelta(seconds=0.05), f'Iteration: {i}')
            ts = ts_2

    async def test_success_preload_limit(self):
        async def gen():
            for i in range(10):
                yield i
                await sleep(0.1)

        queue = AsyncSource[int](source=gen()) >> Queue(queue_size=1)
        queue.start_consumer()

        await sleep(1)
        ts = None

        async for i in queue:
            ts_2 = datetime.now()
            self.assertIsInstance(i, int)
            if ts is not None and i > 1:
                self.assertGreaterEqual(ts_2 - ts, timedelta(seconds=0.1), f'Iteration: {i}')
            ts = ts_2

    async def test_success_stream(self):
        async def gen():
            for i in range(10):
                await sleep(0.1)
                yield i

        queue = AsyncSource[int](source=gen()) >> Queue()
        queue.start_consumer()

        ts = datetime.now()

        async for i in queue:
            ts_2 = datetime.now()
            self.assertIsInstance(i, int)
            self.assertGreaterEqual(ts_2 - ts, timedelta(seconds=0.1), f'Iteration: {i}')
            ts = ts_2

    async def test_success_fail(self):
        async def gen():
            for i in range(10):
                yield i
            raise ValueError()

        queue = AsyncSource[int](source=gen()) >> Queue()
        result = []

        with self.assertRaises(ValueError):
            async for i in queue:
                result.append(i)

        self.assertEqual(result, [i for i in range(10)])

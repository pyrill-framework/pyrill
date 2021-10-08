from asyncio import wait
from unittest import IsolatedAsyncioTestCase

from pyrill.primitives import Cache
from pyrill.sources import SyncSource


class CacheTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        data = [2, 56, 34]

        def gen():
            while True:
                try:
                    yield data.pop(0)
                except IndexError:
                    return

        cache = SyncSource[int](source=gen()) >> Cache()

        self.assertEqual([d async for d in cache], [2, 56, 34], 'First consumption')
        self.assertEqual([d async for d in cache], [2, 56, 34], 'Second consumption')

    async def test_success_concurrent(self):
        data = [2, 56, 34, 232, 23, 45434]

        def gen():
            while True:
                try:
                    yield data.pop(0)
                except IndexError:
                    return

        cache = SyncSource[int](source=gen()) >> Cache()

        async def consumer():
            return [d async for d in cache]

        futs, _ = await wait([consumer() for _ in range(10)])

        self.assertEqual(len(futs), 10)

        for i, f in enumerate(futs):
            self.assertEqual(f.result(), [2, 56, 34, 232, 23, 45434], f'Consumption: {i}')

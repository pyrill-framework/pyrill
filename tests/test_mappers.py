from unittest import IsolatedAsyncioTestCase

from pyrill.mappers import Map, make_map
from pyrill.sources import SyncSource


class MapTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[1, 2, 3])

        stage = Map(source=source, map_func=lambda x: x * 2)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 4, 6])

    async def test_success_async(self):
        source = SyncSource(source=[1, 2, 3])

        async def map(x: int) -> int:
            return x * 2

        stage = Map(source=source, map_func=map)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 4, 6])


class MakeMapTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[1, 2, 3])

        @make_map
        def map(x: int) -> int:
            return x * 2

        stage = map(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 4, 6])

    async def test_success_async(self):
        source = SyncSource(source=[1, 2, 3])

        @make_map
        async def map(x: int) -> int:
            return x * 2

        stage = map(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 4, 6])

    async def test_success_skip_exception(self):
        source = SyncSource(source=[1, 2, 3])

        @make_map
        async def map(x: int) -> int:
            if x % 2 == 0:
                raise ValueError()
            return x * 2

        stage = map(source=source, skip_errors=True)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 6])

    async def test_success_force_stop(self):
        source = SyncSource(source=[1, 2, 3])

        @make_map
        async def map(x: int) -> int:
            if x % 2 == 0:
                raise StopAsyncIteration()
            return x * 2

        stage = map(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [2])

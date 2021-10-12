from unittest import IsolatedAsyncioTestCase

from pyrill.sources import AsyncSource, SyncSource


class SyncSourceTestCase(IsolatedAsyncioTestCase):

    async def test_success_list(self):
        source = SyncSource[int](source=[2, 56, 34])

        self.assertEqual(source.source, [2, 56, 34])
        self.assertEqual([t async for t in source], [2, 56, 34])

    async def test_success_generator(self):
        def gen():
            yield 2
            yield 56
            yield 34

        source = SyncSource[int](source=gen())

        self.assertEqual([t async for t in source], [2, 56, 34])

    async def test_success_iterator(self):
        source = SyncSource[int](source=iter([2, 56, 34]))

        self.assertEqual([t async for t in source], [2, 56, 34])


class AsyncSourceTestCase(IsolatedAsyncioTestCase):

    async def test_success_generator(self):
        async def gen():
            yield 2
            yield 56
            yield 34

        source = AsyncSource[int](source=gen())

        self.assertEqual([t async for t in source], [2, 56, 34])

    async def test_success_iterator(self):
        inner_source = SyncSource[int](source=[2, 56, 34])

        source = AsyncSource[int](source=inner_source)

        self.assertEqual(source.source, inner_source)

        self.assertEqual([t async for t in source], [2, 56, 34])

    async def test_mount_fail(self):
        source = AsyncSource[int]()

        with self.assertRaises(RuntimeError):
            [t async for t in source]

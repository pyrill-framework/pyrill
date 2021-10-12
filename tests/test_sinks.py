from typing import Any
from unittest import IsolatedAsyncioTestCase

from pyrill.base import ElementState
from pyrill.sinks import First, Last
from pyrill.sources import AsyncSource, SyncSource


class FirstTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[2, 56, 34])

        sink = First(source=source)

        self.assertEqual(await sink.get_frame(), 2)

    async def test_no_frames(self):
        source = SyncSource[Any](source=[])

        sink = First(source=source)

        with self.assertRaises(ValueError):
            await sink.get_frame()


class LastTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[2, 56, 34])

        sink = Last(source=source)

        self.assertEqual(await sink.get_frame(), 34)

    async def test_no_frames(self):
        source = SyncSource[Any](source=[])

        sink = Last(source=source)

        with self.assertRaises(ValueError):
            await sink.get_frame()

    async def test_error(self):
        async def it():
            yield 1
            raise ValueError()

        sink = AsyncSource[Any](source=it()) >> Last()

        with self.assertRaises(ValueError):
            await sink.get_frame()

        self.assertEqual(sink.state, ElementState.ERROR)

from typing import Any
from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import ListAcc, SumAcc
from pyrill.sources import SyncSource


class SumAccTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[2, 56, 34])

        stage = SumAcc(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 58, 92])

    async def test_fail(self):
        source = SyncSource[Any](source=[2, 'ere', 56, 'oro', 34])

        stage = SumAcc(source=source)

        with self.assertRaises(TypeError):
            [t async for t in stage]

    async def test_success_str(self):
        source = SyncSource[str](source=['hello ', 'world. ', 'I\'m ', 'a ', 'stream'])

        stage = SumAcc(source=source)

        result = [t async for t in stage]

        self.assertEqual(result,
                         ['hello ',
                          'hello world. ',
                          'hello world. I\'m ',
                          'hello world. I\'m a ',
                          'hello world. I\'m a stream'])


class ListAccTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[2, 56, 34])

        stage = ListAcc(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [[2, ], [2, 56], [2, 56, 34]])

from typing import Any, List
from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import Count, ListAcc, SetAcc, Size, SumAcc
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


class SetAccTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[2, 56, 2, 34])

        stage = SetAcc(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [{2, }, {2, 56}, {2, 56}, {2, 56, 34}])


class CountTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource[int](source=[2, 56, 34])

        stage = Count(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [1, 2, 3])


class SizeTestCase(IsolatedAsyncioTestCase):

    async def test_success_str(self):
        source = SyncSource[str](source=['2', '56', '34'])

        stage = Size(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [1, 3, 5])

    async def test_success_bytes(self):
        source = SyncSource[bytes](source=[b'2', b'56', b'34'])

        stage = Size(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [1, 3, 5])

    async def test_success_list(self):
        source = SyncSource[List[int]](source=[[2, ], [5, 6], [3, 4]])

        stage = Size(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [1, 3, 5])

    async def test_fail_no_size(self):
        source = SyncSource[str](source=['2', 56, '34'])

        stage = Size(source=source)

        with self.assertRaises(TypeError):
            [t async for t in stage]

from unittest import IsolatedAsyncioTestCase

from pyrill.lists import Explode, GetItem, Implode
from pyrill.sources import SyncSource


class ExplodeTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[[1, 2, 3], [4, 5, 6]])

        stage = Explode(source=source)

        result = [t async for t in stage]

        self.assertEqual(result, [1, 2, 3, 4, 5, 6])


class ImplodeTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[1, 2, 3, 4, 5, 6])

        stage = Implode(source=source, max_length=4)

        result = [t async for t in stage]

        self.assertEqual(result, [[1, 2, 3, 4], [5, 6]])


class GetItemCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        source = SyncSource(source=[[1, 2], [3, 4], [5, 6]])

        stage = GetItem(source=source, index=1)

        result = [t async for t in stage]

        self.assertEqual(result, [2, 4, 6])

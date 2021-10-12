from asyncio import ensure_future, wait
from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import ListAcc
from pyrill.primitives import Branch, Cache, PrefixStream, Tee
from pyrill.sinks import BlackHole, Last
from pyrill.sources import SyncSource
from pyrill.utils import SinkConsumer


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


class BranchTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        branch = SyncSource[int](source=[1, 2, 3, 4, 5, 6, 7, 8, 9]) >> Branch(branch_func=lambda frame: frame % 3)

        branch_0 = branch.add_branch(0, ListAcc()) >> Last()
        branch_1 = branch.add_branch(1, ListAcc()) >> Last()
        branch_2 = branch.add_branch(2, ListAcc()) >> Last()

        fut_0 = ensure_future(branch_0.get_frame())
        fut_1 = ensure_future(branch_1.get_frame())
        fut_2 = ensure_future(branch_2.get_frame())

        await wait([fut_0, fut_1, fut_2])

        self.assertEqual(fut_0.result(), [3, 6, 9])
        self.assertEqual(fut_1.result(), [1, 4, 7])
        self.assertEqual(fut_2.result(), [2, 5, 8])

    async def test_success_dynamic(self):
        branch = SyncSource[int](
            source=[
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                13]) >> Branch(
            branch_func=lambda frame: frame %
            3)

        sink_consumer = SinkConsumer()

        @branch.on_unknown_branch
        async def new_branch(br, frame):
            sink = branch.add_branch(br, PrefixStream(prefix=frame)) >> ListAcc() >> Last(name=f'branch_{br}')
            sink_consumer.add_sink(sink)

        sink_consumer.add_sink(branch.add_branch(None, BlackHole()))

        await sink_consumer.wait_until_finish_all()

        for sink in sink_consumer:
            if not isinstance(sink, Last):
                continue

            if sink.name == 'branch_0':
                self.assertEqual(await sink.get_frame(), [3, 6, 9])
            elif sink.name == 'branch_1':
                self.assertEqual(await sink.get_frame(), [1, 4, 7, 10, 13])
            elif sink.name == 'branch_2':
                self.assertEqual(await sink.get_frame(), [2, 5, 8])
            else:
                raise ValueError(sink.name)

        self.assertEqual(len(sink_consumer), 4)

    async def test_success_dynamic_ignore_frames(self):
        branch = SyncSource[int](source=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13]) >> Branch(
            branch_func=lambda frame: frame % 3)

        sink_consumer = SinkConsumer()

        @branch.on_unknown_branch
        async def new_branch(br, frame):
            if br == 2:
                raise ValueError(frame)

            sink = branch.add_branch(br, PrefixStream(prefix=frame)) >> ListAcc() >> Last(name=f'branch_{br}')
            sink_consumer.add_sink(sink)

        sink_consumer.add_sink(branch.add_branch(None, BlackHole()))

        await sink_consumer

        for sink in sink_consumer:
            if not isinstance(sink, Last):
                continue

            if sink.name == 'branch_0':
                self.assertEqual(await sink.get_frame(), [3, 6, 9])
            elif sink.name == 'branch_1':
                self.assertEqual(await sink.get_frame(), [1, 4, 7, 10, 13])
            else:
                raise ValueError(sink.name)

        self.assertEqual(len(sink_consumer), 3)


class TeeTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        tee = SyncSource[int](source=[1, 2, 3]) >> Tee()

        branch_0 = tee >> ListAcc() >> Last()
        branch_1 = tee >> ListAcc() >> Last()
        branch_2 = tee >> ListAcc() >> Last()

        fut_0 = ensure_future(branch_0.get_frame())
        fut_1 = ensure_future(branch_1.get_frame())
        fut_2 = ensure_future(branch_2.get_frame())

        await wait([fut_0, fut_1, fut_2])

        self.assertEqual(fut_0.result(), [1, 2, 3])
        self.assertEqual(fut_1.result(), [1, 2, 3])
        self.assertEqual(fut_2.result(), [1, 2, 3])

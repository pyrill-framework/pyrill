from typing import Any
from unittest import IsolatedAsyncioTestCase

from pyrill.accumulators import ListAcc
from pyrill.base import BaseStage
from pyrill.sinks import BlackHole, First, Last
from pyrill.sources import SyncSource
from pyrill.strlike import Split, Strip


class PipelineTestCase(IsolatedAsyncioTestCase):

    async def test_success(self):
        stage = SyncSource(source=['text to split']) >> Split[str, str](sep=' ')

        self.assertIsInstance(stage, Split)
        result = [t async for t in stage]

        self.assertEqual(result, [['text', 'to', 'split']])

    async def test_success_inverted(self):
        stage = Split[str, str](sep=' ') << SyncSource(source=['text to split'])

        self.assertIsInstance(stage, Split)
        result = [t async for t in stage]

        self.assertEqual(result, [['text', 'to', 'split']])

    async def test_success_full(self):
        sink = SyncSource(source=['text to split']) >> Split[str, str](sep=' ') >> First()

        self.assertIsInstance(sink, First)

        self.assertEqual(await sink.get_frame(), ['text', 'to', 'split'])

    async def test_fail_src_to_src(self):
        with self.assertRaises(TypeError):
            SyncSource(source=['text to split']) >> SyncSource(source=['text to split'])

    async def test_fail_src_to_src_inverted(self):
        with self.assertRaises(TypeError):
            SyncSource(source=['text to split']) << SyncSource(source=['text to split'])

    async def test_fail_sink_to_sink(self):
        with self.assertRaises(TypeError):
            First() >> First()

    async def test_fail_sink_to_sink_inverted(self):
        with self.assertRaises(TypeError):
            First() << First()


class BaseConsumer(IsolatedAsyncioTestCase):

    async def test_no_source_fail(self):
        stage = Strip()

        with self.assertRaises(RuntimeError):
            [_ async for _ in stage]


class BaseSinkTestCase(IsolatedAsyncioTestCase):

    async def test_consume_all(self):
        sink = SyncSource(source=[1, 2, 3]) >> BlackHole()

        sink.consume_all()

        await sink.wait_until_eos()


class BaseStageTestCase(IsolatedAsyncioTestCase):

    async def test_consume_all_fail_last(self):
        class StopStage(BaseStage[Any, Any]):
            async def process_frame(self, frame: Any) -> Any:
                raise StopAsyncIteration()

        sink = SyncSource(source=[1, 2, 3]) >> StopStage() >> ListAcc() >> Last()

        sink.consume_all()

        with self.assertRaises(ValueError):
            await sink.wait_until_eos()

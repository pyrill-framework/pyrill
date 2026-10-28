from unittest.mock import MagicMock

import pytest

from pyrill.base import (
    MSG_ELEMENT_ERROR,
    MSG_ELEMENT_NULL,
    MSG_ELEMENT_READY,
    MSG_STREAM_START,
    MSG_STREAM_STOP,
    ElementState,
    Message,
    Pipeline,
)
from pyrill.mappers import Map
from pyrill.sinks import BlackHole, First
from pyrill.sources import IterableSource


class TestSourceStreamMessages:
    async def test_mount_does_not_start_stream(self) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=[1], name="input", pipeline=pipeline)
        callback = MagicMock(return_value=None)
        source.add_message_callback(callback)

        await source.mount()

        assert source.state == ElementState.READY
        callback.assert_called_once_with(
            source, Message("input", "test", MSG_ELEMENT_READY)
        )

    @pytest.mark.parametrize("remount", [False, True])
    async def test_first_read_emits_start(self, remount: bool) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=[1], name="input", pipeline=pipeline)
        await source.mount()
        if remount:
            await anext(source)
            await source.unmount()
            await source.mount()
        local = MagicMock(return_value=None)
        forwarded = MagicMock(return_value=None)
        source.add_message_callback(local)
        pipeline.add_message_callback(forwarded)
        message = Message("input", "test", MSG_STREAM_START)

        frame = await anext(source)

        assert frame == 1
        assert source.state == ElementState.READY
        local.assert_called_once_with(source, message)
        forwarded.assert_called_once_with(pipeline, message)

    async def test_subsequent_read_does_not_repeat_start(self) -> None:
        source = IterableSource[int](source=[1, 2])
        await source.mount()
        await anext(source)
        callback = MagicMock(return_value=None)
        source.add_message_callback(callback)

        frame = await anext(source)

        assert frame == 2
        assert source.state == ElementState.READY
        callback.assert_not_called()

    @pytest.mark.parametrize("frames", [[], [1]])
    async def test_exhaustion_emits_stop_before_null(self, frames: list[int]) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=frames, name="input", pipeline=pipeline)
        await source.mount()
        for _ in frames:
            await anext(source)
        received: list[Message] = []
        source.add_message_callback(lambda _, message: received.append(message))

        with pytest.raises(StopAsyncIteration):
            await anext(source)

        expected = ([MSG_STREAM_START] if not frames else []) + [
            MSG_STREAM_STOP,
            MSG_ELEMENT_NULL,
        ]
        assert received == [Message("input", "test", event) for event in expected]
        assert source.state == ElementState.NULL

    async def test_read_after_exhaustion_does_not_repeat_stop(self) -> None:
        source = IterableSource[int](source=[])
        await source.mount()
        with pytest.raises(StopAsyncIteration):
            await anext(source)
        callback = MagicMock(return_value=None)
        source.add_message_callback(callback)

        with pytest.raises(StopAsyncIteration):
            await anext(source)

        assert source.state == ElementState.NULL
        callback.assert_not_called()

    async def test_explicit_unmount_does_not_emit_stop(self) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=[1, 2], name="input", pipeline=pipeline)
        await source.mount()
        await anext(source)
        callback = MagicMock(return_value=None)
        source.add_message_callback(callback)

        await source.unmount()

        assert source.state == ElementState.NULL
        callback.assert_called_once_with(
            source, Message("input", "test", MSG_ELEMENT_NULL)
        )


class TestConsumerStreamMessages:
    async def test_consuming_exhausted_source_emits_stop(self) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=[], name="input", pipeline=pipeline)
        sink = source >> BlackHole[int](name="output", pipeline=pipeline)
        await sink.mount()
        callback = MagicMock(return_value=None)
        sink.add_message_callback(callback)

        with pytest.raises(StopAsyncIteration):
            await sink.consume_frame()

        assert source.state == ElementState.NULL
        assert sink.state == ElementState.READY
        callback.assert_called_once_with(
            sink, Message("output", "test", MSG_STREAM_STOP)
        )

    @pytest.mark.parametrize("frames", [[], [1, 2]])
    async def test_full_sink_emits_one_stop_on_exhaustion(
        self, frames: list[int]
    ) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=frames, name="input", pipeline=pipeline)
        sink = source >> BlackHole[int](name="output", pipeline=pipeline)
        received: list[Message] = []
        pipeline.add_message_callback(lambda _, message: received.append(message))

        await sink.consume_all()

        assert received == [
            Message("input", "test", MSG_ELEMENT_READY),
            Message("output", "test", MSG_ELEMENT_READY),
            Message("input", "test", MSG_STREAM_START),
            Message("input", "test", MSG_STREAM_STOP),
            Message("input", "test", MSG_ELEMENT_NULL),
            Message("output", "test", MSG_STREAM_STOP),
            Message("output", "test", MSG_ELEMENT_NULL),
        ]
        assert source.state == sink.state == ElementState.NULL

    async def test_first_sink_unmounts_without_stream_stop(self) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[int](source=[1, 2], pipeline=pipeline)
        sink = source >> First[int](pipeline=pipeline)
        callback = MagicMock(return_value=None)
        pipeline.add_message_callback(callback, MSG_STREAM_STOP)

        frame = await sink.get_frame()

        assert frame == 1
        assert source.state == sink.state == ElementState.NULL
        callback.assert_not_called()

    async def test_failure_emits_error_without_stream_stop(self) -> None:
        pipeline = Pipeline("test")
        source = IterableSource[str](source=["invalid"], pipeline=pipeline)
        stage = source >> Map[str, int](map_func=int, pipeline=pipeline)
        sink = stage >> BlackHole[int](pipeline=pipeline)
        stopped = MagicMock(return_value=None)
        errors = MagicMock(return_value=None)
        pipeline.add_message_callback(stopped, MSG_STREAM_STOP)
        sink.add_message_callback(errors, MSG_ELEMENT_ERROR)

        with pytest.raises(ValueError) as caught:
            await sink.consume_all()

        assert source.state == ElementState.NULL
        assert stage.state == sink.state == ElementState.ERROR
        stopped.assert_not_called()
        errors.assert_called_once_with(
            sink,
            Message(sink.name, "test", MSG_ELEMENT_ERROR, {"ex": caught.value}),
        )

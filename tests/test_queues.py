import pytest

from pyrill.base import ElementState
from pyrill.queues import Queue, QueueSource
from pyrill.sinks import First, Last
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestQueueSource
# ---------------------------------------------------------------------------


class TestQueueSource:
    async def test_mount_transitions_to_ready(self) -> None:
        source: QueueSource[int] = QueueSource()

        await source.mount()

        assert source.state == ElementState.READY

    async def test_unmount_transitions_to_null(self) -> None:
        source: QueueSource[int] = QueueSource()
        await source.mount()

        await source.unmount()

        assert source.state == ElementState.NULL

    async def test_push_frame_makes_frame_available_via_anext(self) -> None:
        source: QueueSource[int] = QueueSource()
        await source.mount()
        await source.push_frame(42)

        result = await source.__anext__()

        assert result == 42
        assert source.state == ElementState.READY

    async def test_push_multiple_frames_consumed_in_fifo_order(self) -> None:
        source: QueueSource[int] = QueueSource()
        await source.mount()
        await source.push_frame(1)
        await source.push_frame(2)
        await source.push_frame(3)

        first = await source.__anext__()
        second = await source.__anext__()
        third = await source.__anext__()

        assert first == 1
        assert second == 2
        assert third == 3
        assert source.state == ElementState.READY

    async def test_push_stop_async_iteration_exhausts_source(self) -> None:
        source: QueueSource[int] = QueueSource()
        await source.mount()
        await source.push_frame(StopAsyncIteration())

        with pytest.raises(StopAsyncIteration):
            await source.__anext__()

        assert source.state == ElementState.NULL

    async def test_push_exception_is_raised_by_anext(self) -> None:
        source: QueueSource[int] = QueueSource()
        await source.mount()
        await source.push_frame(ValueError("boom"))

        with pytest.raises(ValueError, match="boom"):
            await source.__anext__()

    async def test_push_frame_after_stream_closed_raises(self) -> None:
        source: QueueSource[int] = QueueSource()
        await source.mount()
        await source.push_frame(StopAsyncIteration())

        with pytest.raises(RuntimeError, match="Stream already finished"):
            await source.push_frame(99)

    async def test_push_frame_without_mount_raises(self) -> None:
        source: QueueSource[int] = QueueSource()

        with pytest.raises(RuntimeError):
            await source.push_frame(1)


# ---------------------------------------------------------------------------
# TestQueue
# ---------------------------------------------------------------------------


class TestQueue:
    async def test_mount_after_stream_ends_succeeds(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        stage: Queue[int] = source >> Queue[int]()
        await stage.mount()
        _ = [frame async for frame in stage]

        await stage.mount()

        assert stage.state is ElementState.READY

    async def test_last_sink_receives_final_frame(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: Last[int] = source >> Queue[int]() >> Last[int]()

        result = await sink.get_frame()

        assert result == 3

    async def test_first_sink_receives_first_frame(self) -> None:
        source: IterableSource[int] = IterableSource(source=[10, 20, 30])
        sink: First[int] = source >> Queue[int]() >> First[int]()

        result = await sink.get_frame()

        assert result == 10

    async def test_single_frame_source_passes_through(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        sink: Last[int] = source >> Queue[int]() >> Last[int]()

        result = await sink.get_frame()

        assert result == 42

    async def test_empty_source_raises_value_error_on_last(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        sink: Last[int] = source >> Queue[int]() >> Last[int]()

        with pytest.raises(ValueError, match="No frames consumed"):
            await sink.get_frame()

    async def test_empty_source_raises_value_error_on_first(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        sink: First[int] = source >> Queue[int]() >> First[int]()

        with pytest.raises(ValueError, match="No frames consumed"):
            await sink.get_frame()

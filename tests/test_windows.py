from asyncio import CancelledError, Event, create_task, gather, sleep, wait_for
from collections.abc import Callable, Hashable
from contextlib import suppress
from datetime import timedelta
from unittest.mock import Mock, call

import pytest

from pyrill.accumulators import ListAcc
from pyrill.base import (
    MSG_FRAME_SKIPPED,
    BaseProducer,
    ElementState,
    Message,
)
from pyrill.mappers import Map
from pyrill.queues import QueueSource
from pyrill.sinks import BlackHole, Last
from pyrill.sources import IterableSource
from pyrill.windows import (
    GroupBy,
    GroupQueue,
    WindowQueue,
    WindowSize,
    WindowTime,
)


class TestWindowQueue:
    async def test_anext_returns_queued_frame_and_clears_pressure(self) -> None:
        source: WindowQueue[int] = WindowQueue()
        await source.mount()
        await source.push_frame(42)
        try:
            result = await anext(source)

            assert result == 42
            assert not source.pressure
            assert source.state is ElementState.READY
        finally:
            await source.unmount()

    async def test_pending_read_activates_pressure(self) -> None:
        source: WindowQueue[int] = WindowQueue()
        await source.mount()
        task = create_task(anext(source))
        try:
            await sleep(0)

            assert source.pressure
            assert not task.done()
            assert source.state is ElementState.READY
        finally:
            task.cancel()
            with suppress(CancelledError):
                await task
            await source.unmount()

    async def test_iteration_preserves_frame_order(self) -> None:
        source: WindowQueue[int] = WindowQueue()
        await source.mount()
        for frame in [1, 2, 3]:
            await source.push_frame(frame)
        await source.end_of_stream()

        result = [frame async for frame in source]

        assert result == [1, 2, 3]
        assert not source.pressure
        assert source.state is ElementState.NULL

    async def test_end_of_stream_clears_pressure(self) -> None:
        source: WindowQueue[int] = WindowQueue()
        await source.mount()
        await source.end_of_stream()

        with pytest.raises(StopAsyncIteration):
            await anext(source)

        assert not source.pressure
        assert source.state is ElementState.NULL

    async def test_exception_propagates_and_clears_pressure(self) -> None:
        source: WindowQueue[int] = WindowQueue()
        error = ValueError("queue failed")
        await source.mount()
        await source.push_frame(error)
        try:
            with pytest.raises(ValueError) as caught:
                await anext(source)

            assert caught.value is error
            assert not source.pressure
            assert source.state is ElementState.ERROR
        finally:
            await source.unmount()

    async def test_cancelled_read_clears_pressure(self) -> None:
        source: WindowQueue[int] = WindowQueue()
        await source.mount()
        task = create_task(anext(source))
        await sleep(0)
        try:
            task.cancel()
            with pytest.raises(CancelledError):
                await task

            assert task.cancelled()
            assert not source.pressure
            assert source.state is ElementState.READY
        finally:
            await source.unmount()


class TestWindowSize:
    @pytest.mark.parametrize("size", [1, 2, 3])
    def test_connect_sets_source_and_window_size(self, size: int) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = WindowSize[int](size=size)

        result = source >> stage

        assert result is stage
        assert stage.source is source
        assert stage.size == size
        assert not stage.pressure
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize(
        ("frames", "size", "expected"),
        [
            ([], 2, []),
            ([1], 2, [[1]]),
            ([1, 2, 3], 1, [[1], [2], [3]]),
            ([1, 2, 3, 4], 2, [[1, 2], [3, 4]]),
            ([1, 2, 3, 4, 5], 2, [[1, 2], [3, 4], [5]]),
            ([1, 2, 3], 5, [[1, 2, 3]]),
        ],
    )
    async def test_iteration_groups_frames_into_windows(
        self, frames: list[int], size: int, expected: list[list[int]]
    ) -> None:
        source: IterableSource[int] = IterableSource(source=frames)
        stage = source >> WindowSize[int](size=size)  # pyright: ignore[reportAbstractUsage]
        await stage.mount()

        async def collect() -> list[list[int]]:
            results: list[list[int]] = []
            async for window in stage:
                await window.mount()
                try:
                    results.append([frame async for frame in window])
                finally:
                    await window.unmount()
            return results

        try:
            result = await wait_for(collect(), timeout=1)

            assert result == expected
            assert not stage.pressure
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await stage.unmount()

    @pytest.mark.parametrize("size", [0, -1])
    def test_constructor_rejects_nonpositive_size(self, size: int) -> None:
        with pytest.raises(ValueError, match="Size must be positive"):
            WindowSize[int](size=size)

    async def test_completed_windows_remain_readable_after_outer_stream_ends(
        self,
    ) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3, 4, 5])
        stage = source >> WindowSize[int](size=2)
        await stage.mount()

        async def collect() -> list[list[int]]:
            windows = [window async for window in stage]
            return [[frame async for frame in window] for window in windows]

        try:
            result = await wait_for(collect(), timeout=1)

            assert result == [[1, 2], [3, 4], [5]]
            assert stage.state is ElementState.NULL
            assert not stage.pressure
        finally:
            await stage.unmount()

    async def test_iteration_after_remount_starts_new_windows(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> WindowSize[int](size=2)

        async def collect() -> list[list[int]]:
            results: list[list[int]] = []
            async for window in stage:
                results.append([frame async for frame in window])
            return results

        await stage.mount()
        await wait_for(collect(), timeout=1)
        await stage.mount()
        try:
            result = await wait_for(collect(), timeout=1)

            assert result == [[1, 2], [3]]
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
            assert not stage.pressure
        finally:
            await stage.unmount()


def _record_windows(
    stage: WindowTime[int],
    results: list[list[int]],
    completed: Event | None = None,
) -> BlackHole[None]:
    async def record(window: BaseProducer[int]) -> None:
        sink = window >> ListAcc[int]() >> Last[list[int]]()
        results.append(await sink.get_frame())
        if completed is not None:
            completed.set()

    return stage >> Map[BaseProducer[int], None](map_func=record) >> BlackHole[None]()


class TestWindowTime:
    @pytest.mark.parametrize(
        "timeout",
        [timedelta(microseconds=1), timedelta(milliseconds=10), timedelta(seconds=1)],
    )
    def test_connect_sets_source_and_timeout(self, timeout: timedelta) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = WindowTime[int](timeout=timeout)

        result = source >> stage

        assert result is stage
        assert stage.source is source
        assert stage.timeout == timeout
        assert stage.state is ElementState.NULL
        assert not stage.pressure

    @pytest.mark.parametrize(
        "timeout",
        [timedelta(0), timedelta(microseconds=-1), timedelta(seconds=-1)],
    )
    def test_constructor_rejects_nonpositive_timeout(self, timeout: timedelta) -> None:
        with pytest.raises(ValueError, match="timeout must be positive"):
            WindowTime[int](timeout=timeout)

    async def test_mount_transitions_to_ready(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=10))
        try:
            await stage.mount()

            assert stage.state is ElementState.READY
            assert source.state is ElementState.READY
            assert not stage.pressure
        finally:
            await stage.unmount()

    async def test_unmount_stops_idle_consumer(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=10))
        await stage.mount()

        await wait_for(stage.unmount(), timeout=1)

        assert stage.state is ElementState.NULL
        assert source.state is ElementState.NULL
        assert not stage.pressure

    @pytest.mark.parametrize("frames", [[], [1], [1, 2, 3]])
    async def test_source_end_emits_final_window_without_waiting_for_timeout(
        self, frames: list[int]
    ) -> None:
        source: IterableSource[int] = IterableSource(source=frames)
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=50))
        results: list[list[int]] = []
        sink = _record_windows(stage, results)
        try:
            await wait_for(sink.consume_all(), timeout=1)

            assert results == ([frames] if frames else [])
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
            assert not stage.pressure
        finally:
            await sink.unmount()
            # Let outstanding timer callbacks finish before the loop is closed.
            await sleep(0.06)

    async def test_expired_window_closes_while_upstream_remains_open(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=10))
        await stage.mount()
        await source.push_frame(42)
        window = await wait_for(anext(stage), timeout=1)
        sink = window >> ListAcc[int]() >> Last[list[int]]()
        try:
            result = await wait_for(sink.get_frame(), timeout=1)

            assert result == [42]
            assert window.state is ElementState.NULL
            assert stage.state is ElementState.READY
            assert source.state is ElementState.READY
        finally:
            await stage.unmount()
            await sink.unmount()

    async def test_timeout_separates_frames_into_successive_windows(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=10))
        results: list[list[int]] = []
        completed = Event()
        sink = _record_windows(stage, results, completed)
        await sink.mount()

        async def produce() -> None:
            await source.push_frame(1)
            await source.push_frame(2)
            # The next batch starts only after a real timer has closed the first.
            await completed.wait()
            await source.push_frame(3)
            await source.end_of_stream()

        try:
            await wait_for(gather(sink.consume_all(), produce()), timeout=1)

            assert results == [[1, 2], [3]]
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await sink.unmount()
            await sleep(0.02)

    async def test_unmount_closes_partially_filled_window(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=50))
        await stage.mount()
        await source.push_frame(7)
        window = await wait_for(anext(stage), timeout=1)
        sink = window >> ListAcc[int]() >> Last[list[int]]()
        await stage.unmount()
        try:
            result = await wait_for(sink.get_frame(), timeout=1)

            assert result == [7]
            assert window.state is ElementState.NULL
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await sink.unmount()
            await sleep(0.06)

    async def test_consumption_after_remount_starts_new_windows(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> WindowTime[int](timeout=timedelta(milliseconds=50))
        results: list[list[int]] = []
        sink = _record_windows(stage, results)
        await wait_for(sink.consume_all(), timeout=1)
        results.clear()
        try:
            await wait_for(sink.consume_all(), timeout=1)

            assert results == [[1, 2, 3]]
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await sink.unmount()
            await sleep(0.06)


def _identity(frame: int) -> int:
    return frame


def _parity(frame: int) -> int:
    return frame % 2


def _constant_key(frame: int) -> None:
    return None


def _tuple_key(frame: int) -> tuple[int, str]:
    return frame % 2, "key"


def _record_groups(stage: GroupBy[int], results: list[list[int]]) -> BlackHole[None]:
    async def record(group: BaseProducer[int]) -> None:
        sink = group >> ListAcc[int]() >> Last[list[int]]()
        results.append(await sink.get_frame())

    return stage >> Map[BaseProducer[int], None](map_func=record) >> BlackHole[None]()


class TestGroupQueue:
    @pytest.mark.parametrize(
        "key",
        [None, 0, False, "", "odd", (1, "key"), frozenset({1, 2})],
        ids=["none", "zero", "false", "empty-string", "string", "tuple", "frozenset"],
    )
    def test_key_returns_supplied_object(self, key: Hashable) -> None:
        group: GroupQueue[int] = GroupQueue(key=key)

        result = group.key

        assert result is key
        assert group.state is ElementState.NULL

    async def test_consumption_preserves_key(self) -> None:
        key = (1, "odd")
        group: GroupQueue[int] = GroupQueue(key=key)
        await group.mount()
        await group.push_frame(1)
        await group.push_frame(3)
        await group.end_of_stream()
        sink = group >> ListAcc[int]() >> Last[list[int]]()

        result = await wait_for(sink.get_frame(), timeout=1)

        assert result == [1, 3]
        assert group.key is key
        assert group.state is ElementState.NULL

    async def test_remount_preserves_key(self) -> None:
        key = (1, "odd")
        group: GroupQueue[int] = GroupQueue(key=key)
        await group.mount()
        await group.unmount()
        try:
            await group.mount()

            assert group.key is key
            assert group.state is ElementState.READY
        finally:
            await group.unmount()


class TestGroupBy:
    @pytest.mark.parametrize(
        ("key_builder", "expected"),
        [
            (_parity, [(1, [1, 3]), (0, [2])]),
            (_identity, [(1, [1]), (2, [2]), (3, [3])]),
            (_constant_key, [(None, [1, 2, 3])]),
            (_tuple_key, [((1, "key"), [1, 3]), ((0, "key"), [2])]),
        ],
        ids=["parity", "identity", "none", "tuple"],
    )
    async def test_emitted_group_keys_match_their_frames(
        self,
        key_builder: Callable[[int], Hashable],
        expected: list[tuple[Hashable, list[int]]],
    ) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> GroupBy[int](key_builder=key_builder)
        results: list[tuple[Hashable, list[int]]] = []

        async def record(group: BaseProducer[int]) -> None:
            assert isinstance(group, GroupQueue)
            key = group.key
            group_sink = group >> ListAcc[int]() >> Last[list[int]]()
            frames = await group_sink.get_frame()
            assert group.key is key
            results.append((key, frames))

        sink = (
            stage >> Map[BaseProducer[int], None](map_func=record) >> BlackHole[None]()
        )
        try:
            await wait_for(sink.consume_all(), timeout=1)

            assert results == expected
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await sink.unmount()

    def test_connect_sets_source_and_preserves_name(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = GroupBy[int](name="parity", key_builder=_parity)

        result = source >> stage

        assert result is stage
        assert stage.source is source
        assert stage.name == "parity"
        assert stage.state is ElementState.NULL
        assert not stage.pressure

    async def test_mount_transitions_to_ready(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> GroupBy[int](key_builder=_parity)
        try:
            await stage.mount()

            assert stage.state is ElementState.READY
            assert source.state is ElementState.READY
            assert not stage.pressure
        finally:
            await stage.unmount()

    async def test_unmount_stops_idle_consumer(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> GroupBy[int](key_builder=_parity)
        await stage.mount()

        await wait_for(stage.unmount(), timeout=1)

        assert stage.state is ElementState.NULL
        assert source.state is ElementState.NULL
        assert not stage.pressure

    @pytest.mark.parametrize(
        ("frames", "key_builder", "expected"),
        [
            ([], _identity, []),
            ([7], _identity, [[7]]),
            ([1, 3, 5], _parity, [[1, 3, 5]]),
            ([3, 1, 2], _identity, [[3], [1], [2]]),
            ([1, 2, 3, 4, 5], _parity, [[1, 3, 5], [2, 4]]),
            ([2, 1, 4, 3, 6], _parity, [[2, 4, 6], [1, 3]]),
            ([1, 1, 2, 1, 2], _identity, [[1, 1, 1], [2, 2]]),
            ([1, 2, 3], _constant_key, [[1, 2, 3]]),
            ([1, 2, 3, 4], _tuple_key, [[1, 3], [2, 4]]),
        ],
        ids=[
            "empty",
            "single-frame",
            "same-key",
            "distinct-keys",
            "interleaved-keys",
            "first-seen-key-order",
            "duplicate-frames",
            "none-key",
            "tuple-key",
        ],
    )
    async def test_consumption_groups_frames_by_key(
        self,
        frames: list[int],
        key_builder: Callable[[int], Hashable],
        expected: list[list[int]],
    ) -> None:
        source: IterableSource[int] = IterableSource(source=frames)
        stage = source >> GroupBy[int](key_builder=key_builder)
        results: list[list[int]] = []
        sink = _record_groups(stage, results)
        try:
            await wait_for(sink.consume_all(), timeout=1)

            assert results == expected
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
            assert sink.state is ElementState.NULL
            assert not stage.pressure
        finally:
            await sink.unmount()

    async def test_consumption_calls_key_builder_once_per_frame(self) -> None:
        key_builder = Mock(side_effect=_parity)
        source: IterableSource[int] = IterableSource(source=[1, 2, 3, 4])
        stage = source >> GroupBy[int](key_builder=key_builder)
        results: list[list[int]] = []
        sink = _record_groups(stage, results)
        try:
            await wait_for(sink.consume_all(), timeout=1)

            assert key_builder.call_args_list == [call(1), call(2), call(3), call(4)]
            assert results == [[1, 3], [2, 4]]
            assert stage.state is ElementState.NULL
        finally:
            await sink.unmount()

    async def test_groups_can_be_consumed_after_outer_stream_ends(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3, 4, 5])
        stage = source >> GroupBy[int](key_builder=_parity)
        sink = stage >> ListAcc[BaseProducer[int]]() >> Last[list[BaseProducer[int]]]()

        async def collect() -> list[list[int]]:
            groups = await sink.get_frame()

            async def collect_group(group: BaseProducer[int]) -> list[int]:
                group_sink = group >> ListAcc[int]() >> Last[list[int]]()
                return await group_sink.get_frame()

            return list(await gather(*(collect_group(group) for group in groups)))

        try:
            result = await wait_for(collect(), timeout=1)

            assert result == [[1, 3, 5], [2, 4]]
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await sink.unmount()

    async def test_consumption_after_remount_recreates_groups(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> GroupBy[int](key_builder=_parity)
        results: list[list[int]] = []
        sink = _record_groups(stage, results)
        try:
            await wait_for(sink.consume_all(), timeout=1)
            results.clear()

            await wait_for(sink.consume_all(), timeout=1)

            assert results == [[1, 3], [2]]
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
        finally:
            await sink.unmount()

    @pytest.mark.parametrize(
        ("frames", "expected", "skipped"),
        [
            ([0], [], [0]),
            ([0, 1, 2], [[1], [2]], [0]),
            ([1, 0, 2, 3], [[1, 3], [2]], [0]),
            ([1, 2, 0], [[1], [2]], [0]),
            ([0, 1, 0, 2], [[1], [2]], [0, 0]),
        ],
        ids=["all-skipped", "first-frame", "middle-frame", "last-frame", "multiple"],
    )
    async def test_key_builder_error_skips_frame_and_emits_message(
        self, frames: list[int], expected: list[list[int]], skipped: list[int]
    ) -> None:
        error = ValueError("invalid group key")

        def build_key(frame: int) -> int:
            if frame == 0:
                raise error
            return frame % 2

        received: list[Message] = []

        key_builder = Mock(side_effect=build_key)
        source: IterableSource[int] = IterableSource(source=frames)
        stage = source >> GroupBy[int](key_builder=key_builder)
        stage.add_message_callback(
            lambda _, message: received.append(message), MSG_FRAME_SKIPPED
        )
        results: list[list[int]] = []
        sink = _record_groups(stage, results)
        try:
            await wait_for(sink.consume_all(), timeout=1)

            assert results == expected
            assert key_builder.call_args_list == [call(frame) for frame in frames]
            assert [message.params for message in received] == [
                {"frame": frame} for frame in skipped
            ]
            assert stage.state is ElementState.NULL
            assert source.state is ElementState.NULL
            assert sink.state is ElementState.NULL
            assert not stage.pressure
        finally:
            await sink.unmount()

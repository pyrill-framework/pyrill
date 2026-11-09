from asyncio import CancelledError, Event, create_task, sleep, wait_for
from contextlib import suppress

import pytest

from pyrill.base import ElementState
from pyrill.sources import IterableSource
from pyrill.windows import (
    PressureCtx,
    PressureMixin,
    WindowQueue,
    WindowSize,
    active_pressure,
)


class TestPressureMixin:
    def test_initial_pressure_is_inactive(self) -> None:
        element = PressureMixin()

        assert not element.pressure
        assert element.state is ElementState.NULL

    async def test_mount_clears_pressure(self) -> None:
        element = PressureMixin()
        with PressureCtx(element):
            await element.mount()

            assert not element.pressure
            assert element.state is ElementState.READY
        await element.unmount()

    async def test_unmount_clears_pressure(self) -> None:
        element = PressureMixin()
        await element.mount()
        with PressureCtx(element):
            await element.unmount()

            assert not element.pressure
            assert element.state is ElementState.NULL


class TestPressureCtx:
    def test_enter_activates_pressure_and_returns_context(self) -> None:
        element = PressureMixin()
        context = PressureCtx(element)

        with context as result:
            assert result is context
            assert result.elem is element
            assert element.pressure

    def test_exit_clears_pressure(self) -> None:
        element = PressureMixin()

        with PressureCtx(element):
            pass

        assert not element.pressure

    def test_exit_propagates_exception_and_clears_pressure(self) -> None:
        element = PressureMixin()
        error = ValueError("context failed")

        with pytest.raises(ValueError) as caught, PressureCtx(element):
            raise error

        assert caught.value is error
        assert not element.pressure


class TestActivePressure:
    async def test_call_forwards_arguments_and_returns_result(self) -> None:
        element = PressureMixin()
        observed: list[tuple[PressureMixin, int, str, int]] = []

        @active_pressure
        async def operation(subject: PressureMixin, value: int, *, suffix: str) -> str:
            observed.append((subject, value, suffix, subject.pressure))
            return f"{value}{suffix}"

        result = await operation(element, 42, suffix="!")

        assert result == "42!"
        assert observed == [(element, 42, "!", True)]
        assert not element.pressure

    async def test_pressure_remains_active_while_coroutine_waits(self) -> None:
        element = PressureMixin()
        entered = Event()
        release = Event()

        @active_pressure
        async def operation(subject: PressureMixin) -> None:
            entered.set()
            await release.wait()

        task = create_task(operation(element))
        try:
            await wait_for(entered.wait(), timeout=1)

            assert element.pressure
            assert not task.done()
        finally:
            release.set()
            await task

    async def test_exception_propagates_and_clears_pressure(self) -> None:
        element = PressureMixin()
        error = ValueError("operation failed")

        @active_pressure
        async def operation(subject: PressureMixin) -> None:
            raise error

        with pytest.raises(ValueError) as caught:
            await operation(element)

        assert caught.value is error
        assert not element.pressure

    async def test_cancellation_clears_pressure(self) -> None:
        element = PressureMixin()
        entered = Event()
        release = Event()

        @active_pressure
        async def operation(subject: PressureMixin) -> None:
            entered.set()
            await release.wait()

        task = create_task(operation(element))
        await wait_for(entered.wait(), timeout=1)

        task.cancel()
        with pytest.raises(CancelledError):
            await task

        assert task.cancelled()
        assert not element.pressure


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
        stage = source >> WindowSize[int](size=size)
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

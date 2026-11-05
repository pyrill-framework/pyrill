from asyncio import Event, Future, Task, get_running_loop, sleep, wait_for
from collections.abc import Callable, Coroutine, Iterator
from contextvars import ContextVar
from typing import Any
from unittest.mock import Mock

import pytest

from pyrill.base import ElementState
from pyrill.futures import AsCompleted, BaseCreateTask, CreateTask, create_task
from pyrill.sources import IterableSource


class RecordingTask(BaseCreateTask[int, str]):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.builder = Mock(side_effect=self.transform)
        self.release = Event()

    async def transform(self, frame: int) -> str:
        await self.release.wait()
        return str(frame)

    def _create_coroutine(self, frame: int) -> Coroutine[None, None, str]:
        return self.builder(frame)


class TestBaseCreateTask:
    async def test_anext_schedules_coroutine_without_waiting_for_result(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        stage: RecordingTask = source >> RecordingTask()
        await stage.mount()
        try:
            result = await wait_for(anext(stage), timeout=1)

            assert isinstance(result, Task)
            assert result.get_loop() is stage.pipeline.loop
            assert not result.done()
            stage.builder.assert_called_once_with(42)
            assert stage.state is ElementState.READY
        finally:
            stage.release.set()
            await stage.unmount()

    async def test_returned_future_resolves_to_coroutine_result(self) -> None:
        source: IterableSource[int] = IterableSource(source=[7])
        stage: RecordingTask = source >> RecordingTask()
        await stage.mount()
        future = await anext(stage)
        stage.release.set()
        try:
            result = await wait_for(future, timeout=1)

            assert result == "7"
            assert future.done()
            assert not future.cancelled()
        finally:
            await stage.unmount()

    async def test_task_preserves_context_after_caller_changes_it(self) -> None:
        context = ContextVar[str]("test_futures_context", default="default")
        release = Event()

        class ContextTask(BaseCreateTask[int, str]):
            async def read_context(self, frame: int) -> str:
                await release.wait()
                return f"{frame}:{context.get()}"

            def _create_coroutine(self, frame: int) -> Coroutine[None, None, str]:
                return self.read_context(frame)

        token = context.set("original")
        source: IterableSource[int] = IterableSource(source=[3])
        stage: ContextTask = source >> ContextTask()
        await stage.mount()
        future = await anext(stage)
        context.set("changed")
        release.set()
        try:
            result = await wait_for(future, timeout=1)

            assert result == "3:original"
            assert context.get() == "changed"
        finally:
            context.reset(token)
            await stage.unmount()

    async def test_anext_propagates_synchronous_builder_exception(self) -> None:
        error = ValueError("task failed")
        source: IterableSource[int] = IterableSource(source=[1])
        stage: RecordingTask = source >> RecordingTask()
        stage.builder.side_effect = error
        await stage.mount()
        # A synchronous builder failure belongs to the stage, before task creation.
        try:
            with pytest.raises(ValueError) as caught:
                await anext(stage)

            assert caught.value is error
            assert stage.state is ElementState.ERROR
            stage.builder.assert_called_once_with(1)
        finally:
            await stage.unmount()


async def stringify(frame: int) -> str:
    return str(frame)


class TestCreateTask:
    def test_connect_sets_source_and_preserves_name(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        stage = CreateTask[int, str](name="convert", coroutine_builder_func=stringify)

        result = source >> stage

        assert result is stage
        assert stage.name == "convert"
        assert stage.source is source
        assert stage.state is ElementState.NULL

    async def test_anext_uses_supplied_coroutine_builder(self) -> None:
        source: IterableSource[int] = IterableSource(source=[9])
        stage: CreateTask[int, str] = source >> CreateTask[int, str](
            coroutine_builder_func=stringify
        )
        await stage.mount()
        try:
            future = await anext(stage)

            assert isinstance(future, Task)
            assert await wait_for(future, timeout=1) == "9"
        finally:
            await stage.unmount()


class TestCreateTaskFactory:
    @pytest.mark.parametrize("name", [None, "custom", ""])
    def test_direct_call_creates_named_stage(self, name: str | None) -> None:
        stage = create_task(name, stringify)

        assert isinstance(stage, CreateTask)
        assert stage.name == (name or "stringify")
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize("name", [None, "custom", ""])
    def test_decorator_creates_named_stage(self, name: str | None) -> None:
        decorate: Callable[
            [Callable[[int], Coroutine[None, None, str]]], CreateTask[int, str]
        ] = create_task(name)

        stage = decorate(stringify)

        assert isinstance(stage, CreateTask)
        assert stage.name == (name or "stringify")
        assert stage.state is ElementState.NULL


class TestAsCompleted:
    @pytest.mark.parametrize("max_pending", [1, 2, 3])
    async def test_mount_limits_upstream_consumption(self, max_pending: int) -> None:
        futures: list[Future[int]] = [
            get_running_loop().create_future() for _ in range(max_pending + 3)
        ]
        consumed: list[Future[int]] = []

        def frames() -> Iterator[Future[int]]:
            for future in futures:
                consumed.append(future)
                yield future

        source: IterableSource[Future[int]] = IterableSource(source=frames())
        stage: AsCompleted[int] = source >> AsCompleted[int](max_pending=max_pending)
        try:
            await stage.mount()
            # Let the background consumer run until it waits for capacity.
            await sleep(0)

            # One extra frame may be fetched before waiting to enqueue it.
            assert consumed == futures[: max_pending + 1]
            assert all(not future.done() for future in futures)
            assert stage.state is ElementState.READY
        finally:
            await stage.unmount()
            for future in futures:
                future.cancel()

    @pytest.mark.parametrize("max_pending", [None, 1, 2, 3])
    async def test_iteration_drains_more_frames_than_pending_limit(
        self, max_pending: int | None
    ) -> None:
        futures: list[Future[int]] = []
        for value in range(6):
            future: Future[int] = get_running_loop().create_future()
            future.set_result(value)
            futures.append(future)
        source: IterableSource[Future[int]] = IterableSource(source=futures)
        stage: AsCompleted[int] = source >> AsCompleted[int](max_pending=max_pending)
        await stage.mount()

        async def collect() -> list[int]:
            return [value async for value in stage]

        try:
            result = await wait_for(collect(), timeout=1)

            assert sorted(result) == list(range(6))
            assert stage.state is ElementState.NULL
            assert all(future.done() and not future.cancelled() for future in futures)
        finally:
            await stage.unmount()

    @pytest.mark.parametrize("max_pending", [1, 2, 3])
    async def test_unmount_cancels_buffered_futures_when_capacity_is_full(
        self, max_pending: int
    ) -> None:
        futures: list[Future[int]] = [
            get_running_loop().create_future() for _ in range(max_pending + 3)
        ]
        source: IterableSource[Future[int]] = IterableSource(source=futures)
        stage: AsCompleted[int] = source >> AsCompleted[int](max_pending=max_pending)
        await stage.mount()
        await sleep(0)
        try:
            await wait_for(stage.unmount(), timeout=1)

            assert stage.state is ElementState.NULL
            assert all(future.cancelled() for future in futures[:max_pending])
            assert all(not future.done() for future in futures[max_pending:])
        finally:
            await stage.unmount()
            for future in futures:
                future.cancel()

    @pytest.mark.parametrize("max_pending", [None, 1, 3])
    def test_constructor_accepts_pending_limit(self, max_pending: int | None) -> None:
        stage = AsCompleted[int](name="completed", max_pending=max_pending)

        assert stage.name == "completed"
        assert stage.state is ElementState.NULL

    async def test_anext_returns_completed_future_before_pending_future(self) -> None:
        pending: Future[int] = get_running_loop().create_future()
        completed: Future[int] = get_running_loop().create_future()
        completed.set_result(42)
        source: IterableSource[Future[int]] = IterableSource(
            source=[pending, completed]
        )
        stage: AsCompleted[int] = source >> AsCompleted[int]()
        await stage.mount()
        try:
            result = await wait_for(anext(stage), timeout=1)

            assert result == 42
            assert not pending.done()
        finally:
            await stage.unmount()

    async def test_empty_source_ends_iteration(self) -> None:
        source: IterableSource[Future[int]] = IterableSource(source=[])
        stage: AsCompleted[int] = source >> AsCompleted[int]()
        await stage.mount()
        try:
            with pytest.raises(StopAsyncIteration):
                await wait_for(anext(stage), timeout=1)

            assert stage.state is ElementState.NULL
        finally:
            await stage.unmount()

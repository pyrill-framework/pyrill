import asyncio
import subprocess
import sys
from contextlib import suppress
from textwrap import dedent
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pyrill.base import (
    MSG_ELEMENT_ERROR,
    MSG_ELEMENT_NULL,
    MSG_ELEMENT_READY,
    MSG_FRAME_SKIPPED,
    BaseElement,
    BaseFullSink,
    BaseParallelProducer,
    BaseProducer,
    BaseSink,
    BaseStage,
    ElementState,
    FrameSkipped,
    Message,
    Pipeline,
    PressureCtx,
    active_pressure,
)

# ---------------------------------------------------------------------------
# Concrete minimal implementations used as test fixtures
# ---------------------------------------------------------------------------


class ConcreteElement(BaseElement):
    """Minimal concrete BaseElement — no abstract methods to implement."""


class FailingMountElement(BaseElement):
    """BaseElement whose _mount always raises, for error-state testing."""

    async def _mount(self) -> None:
        raise ValueError("mount failed")


class ConcreteProducer(BaseProducer[int]):
    """Producer backed by a fixed list of frames."""

    def __init__(self, frames: list[int], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._test_frames = iter(frames)

    async def _next_frame(self) -> int:
        try:
            return next(self._test_frames)
        except StopIteration:
            raise StopAsyncIteration()


class SkippingProducer(BaseProducer[int]):
    """Producer that raises FrameSkipped on the first call, then returns 42."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._calls = 0

    async def _next_frame(self) -> int:
        self._calls += 1
        if self._calls == 1:
            raise FrameSkipped(frame=0)
        return 42


class PressureProducer(BaseProducer[int]):
    """Producer whose reads wait for a frame supplied by the test."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.frames: asyncio.Queue[int | BaseException] = asyncio.Queue()
        self.reading = asyncio.Event()
        self.observed_pressure: list[bool] = []

    async def _next_frame(self) -> int:
        self.observed_pressure.append(self.pressure)
        self.reading.set()
        frame = await self.frames.get()
        if isinstance(frame, BaseException):
            raise frame
        return frame


class ConcreteSink(BaseSink[int]):
    """Minimal concrete BaseSink — no abstract methods to implement."""


class ConcreteFullSink(BaseFullSink[int]):
    """Full sink that records every processed frame."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.processed: list[int] = []

    async def _process_frame(self, frame: int) -> None:
        self.processed.append(frame)


class ConcreteStage(BaseStage[int, str]):
    """Stage that converts each integer frame to its string representation."""

    async def _process_frame(self, frame: int) -> str:
        return str(frame)


class StrSink(BaseSink[str]):
    """Minimal concrete BaseSink accepting string frames."""


class IntStage(BaseStage[int, int]):
    """Stage that passes integer frames through unchanged."""

    async def _process_frame(self, frame: int) -> int:
        return frame


class ConcreteParallelProducer(BaseParallelProducer[int]):
    """Parallel producer backed by a fixed list of frames."""

    def __init__(self, frames: list[int], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._test_frames = iter(frames)

    async def _next_frame(self) -> int:
        try:
            return next(self._test_frames)
        except StopIteration:
            raise StopAsyncIteration()


class ControlledParallelProducer(BaseParallelProducer[int]):
    """Parallel producer with observable, explicitly released frame reads."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.frames: asyncio.Queue[int | BaseException] = asyncio.Queue()
        self.reading = asyncio.Event()
        self.interrupted = asyncio.Event()
        self.calls = 0
        self.active_reads = 0
        self.max_active_reads = 0
        self.observed_pressure: list[bool] = []

    async def _next_frame(self) -> int:
        self.calls += 1
        self.active_reads += 1
        self.max_active_reads = max(self.max_active_reads, self.active_reads)
        self.observed_pressure.append(self.pressure)
        self.reading.set()
        try:
            frame = await self.frames.get()
            if isinstance(frame, BaseException):
                raise frame
            return frame
        except asyncio.CancelledError:
            self.interrupted.set()
            raise
        finally:
            self.active_reads -= 1


# ---------------------------------------------------------------------------
# TestFrameSkipped
# ---------------------------------------------------------------------------


class TestFrameSkipped:
    def test_default_frame_is_none(self) -> None:
        # Act
        ex = FrameSkipped()

        # Assert
        assert ex.frame is None

    def test_stores_provided_frame(self) -> None:
        # Act
        ex = FrameSkipped(frame=42)

        # Assert
        assert ex.frame == 42

    def test_is_base_exception(self) -> None:
        # Act
        ex = FrameSkipped()

        # Assert
        assert isinstance(ex, BaseException)


# ---------------------------------------------------------------------------
# TestMessage
# ---------------------------------------------------------------------------


class TestMessage:
    def test_stores_sender_pipeline_type_and_params(self) -> None:
        # Act
        msg = Message(
            sender="elem-1",
            pipeline="pipe-1",
            type="some-event",
            params={"key": "val"},
        )

        # Assert
        assert msg.sender == "elem-1"
        assert msg.pipeline == "pipe-1"
        assert msg.type == "some-event"
        assert msg.params == {"key": "val"}

    def test_params_defaults_to_empty_dict(self) -> None:
        # Act
        msg = Message(sender="elem-1", pipeline="pipe-1", type="some-event")

        # Assert
        assert msg.params == {}


# ---------------------------------------------------------------------------
# TestPipeline
# ---------------------------------------------------------------------------


class TestPipeline:
    def test_init_uses_provided_name(self) -> None:
        # Act
        pipeline = Pipeline("my-pipeline")

        # Assert
        assert pipeline.name == "my-pipeline"

    def test_init_auto_generates_non_empty_name(self) -> None:
        # Act
        pipeline = Pipeline()

        # Assert
        assert pipeline.name
        assert isinstance(pipeline.name, str)

    def test_loop_property_returns_provided_loop(self) -> None:
        # Arrange
        loop = asyncio.new_event_loop()

        # Act
        pipeline = Pipeline("test", loop=loop)

        # Assert
        assert pipeline.loop is loop
        loop.close()

    def test_include_element_registers_element(self) -> None:
        # Arrange
        pipeline = Pipeline("test")

        # Act — ConcreteElement.__init__ calls pipeline.include_element
        element = ConcreteElement(pipeline=pipeline)

        # Assert
        assert id(element) in pipeline.elements

    def test_remove_element_deregisters_element(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        element = ConcreteElement(pipeline=pipeline)

        # Act
        pipeline.remove_element(element)

        # Assert
        assert id(element) not in pipeline.elements

    async def test_create_orphan_task_runs_coroutine_and_returns_task(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        executed: list[bool] = []

        async def coro() -> None:
            executed.append(True)

        # Act
        task = pipeline.create_orphan_task(coro())
        await task

        # Assert
        assert isinstance(task, asyncio.Task)
        assert task.done()
        assert executed == [True]


class TestPipelineMessages:
    def test_send_message_calls_matching_callback(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        callback = MagicMock(return_value=None)
        pipeline.add_message_callback(callback, "some-event")
        msg = Message(sender="elem-1", pipeline="test", type="some-event")

        # Act
        pipeline.send_message(msg)

        # Assert
        callback.assert_called_once_with(pipeline, msg)

    def test_send_message_skips_callback_of_other_type(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        callback = MagicMock(return_value=None)
        pipeline.add_message_callback(callback, "other-event")

        # Act
        pipeline.send_message(
            Message(sender="elem-1", pipeline="test", type="some-event")
        )

        # Assert
        callback.assert_not_called()

    def test_send_message_logs_message_at_debug_level(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        msg = Message(sender="elem-1", pipeline="test", type="some-event")

        # Act
        with patch.object(pipeline, "_logger") as logger:
            pipeline.send_message(msg)

        # Assert
        logger.debug.assert_called_once_with("Sending message: %s", msg)

    async def test_element_message_reaches_pipeline_callback(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        element = ConcreteElement(name="elem-1", pipeline=pipeline)
        received: list[Message] = []
        pipeline.add_message_callback(
            lambda _, msg: received.append(msg), MSG_ELEMENT_READY
        )

        # Act
        await element.mount()

        # Assert
        assert received == [
            Message(
                sender="elem-1",
                pipeline="test",
                type=MSG_ELEMENT_READY,
                params={},
            )
        ]


# ---------------------------------------------------------------------------
# TestBaseElement
# ---------------------------------------------------------------------------


class TestBaseElementInit:
    def test_init_with_explicit_name(self) -> None:
        # Act
        element = ConcreteElement(name="my-element")

        # Assert
        assert element.name == "my-element"

    def test_init_auto_name_includes_class_name(self) -> None:
        # Act
        element = ConcreteElement()

        # Assert
        assert element.name.startswith("ConcreteElement-")

    def test_init_registers_with_pipeline(self) -> None:
        # Arrange
        pipeline = Pipeline("test")

        # Act
        element = ConcreteElement(pipeline=pipeline)

        # Assert
        assert id(element) in pipeline.elements

    def test_init_state_is_null(self) -> None:
        # Act
        element = ConcreteElement()

        # Assert
        assert element.state == ElementState.NULL

    def test_pipeline_property_returns_pipeline(self) -> None:
        # Arrange
        pipeline = Pipeline("test")

        # Act
        element = ConcreteElement(pipeline=pipeline)

        # Assert
        assert element.pipeline is pipeline

    def test_new_name_classmethod_includes_class_name(self) -> None:
        # Act
        name = ConcreteElement.new_name()

        # Assert
        assert name.startswith("ConcreteElement-")


class TestBaseElementOperators:
    def test_rshift_raises_type_error(self) -> None:
        # Arrange
        e1 = ConcreteElement()
        e2 = ConcreteElement()

        # Act / Assert
        with pytest.raises(TypeError):
            _ = e1 >> e2  # type: ignore[operator]

    def test_lshift_raises_type_error(self) -> None:
        # Arrange
        e1 = ConcreteElement()
        e2 = ConcreteElement()

        # Act / Assert
        with pytest.raises(TypeError):
            _ = e1 << e2  # type: ignore[operator]


class TestBaseElementMount:
    async def test_mount_transitions_to_ready_and_sends_message(self) -> None:
        # Arrange
        element = ConcreteElement()
        messages: list[Message] = []
        element.add_message_callback(lambda _, msg: messages.append(msg))

        # Act
        await element.mount()

        # Assert
        assert element.state == ElementState.READY
        assert any(m.type == MSG_ELEMENT_READY for m in messages)

    async def test_mount_is_idempotent(self) -> None:
        # Arrange
        element = ConcreteElement()
        await element.mount()

        # Act — second call should be a no-op
        await element.mount()

        # Assert
        assert element.state == ElementState.READY

    async def test_mount_raises_if_in_error_state(self) -> None:
        # Arrange — get the element into ERROR state
        element = FailingMountElement()
        with suppress(ValueError):
            await element.mount()
        assert element.state == ElementState.ERROR

        # Act / Assert
        with pytest.raises(RuntimeError, match="Element has an error"):
            await element.mount()

        assert element.state == ElementState.ERROR

    async def test_mount_sets_error_state_when_mount_hook_fails(self) -> None:
        # Arrange
        element = FailingMountElement()

        # Act
        with pytest.raises(ValueError, match="mount failed"):
            await element.mount()

        # Assert
        assert element.state == ElementState.ERROR


class TestBaseElementUnmount:
    async def test_unmount_transitions_to_null_and_sends_message(self) -> None:
        # Arrange
        element = ConcreteElement()
        await element.mount()
        messages: list[Message] = []
        element.add_message_callback(lambda _, msg: messages.append(msg))

        # Act
        await element.unmount()

        # Assert
        assert element.state == ElementState.NULL
        assert any(m.type == MSG_ELEMENT_NULL for m in messages)

    async def test_unmount_is_noop_when_not_ready(self) -> None:
        # Arrange
        element = ConcreteElement()
        assert element.state == ElementState.NULL

        # Act
        await element.unmount()

        # Assert — stays NULL, no error raised
        assert element.state == ElementState.NULL


class TestBaseElementMessageCallbacks:
    async def test_callback_called_for_matching_message_type(self) -> None:
        # Arrange
        element = ConcreteElement()
        callback = MagicMock(return_value=None)
        element.add_message_callback(callback, MSG_ELEMENT_READY)

        # Act
        await element.mount()

        # Assert
        callback.assert_called_once()
        _, msg = callback.call_args[0]
        assert msg.type == MSG_ELEMENT_READY

    async def test_callback_not_called_for_different_message_type(self) -> None:
        # Arrange
        element = ConcreteElement()
        callback = MagicMock(return_value=None)
        element.add_message_callback(callback, MSG_ELEMENT_NULL)

        # Act — mount sends MSG_ELEMENT_READY, not MSG_ELEMENT_NULL
        await element.mount()

        # Assert
        callback.assert_not_called()

    async def test_callback_with_no_type_filter_receives_all_messages(self) -> None:
        # Arrange
        element = ConcreteElement()
        callback = MagicMock(return_value=None)
        element.add_message_callback(callback)  # no type filter

        # Act
        await element.mount()

        # Assert
        callback.assert_called_once()

    async def test_callback_removed_after_returning_true(self) -> None:
        # Arrange
        element = ConcreteElement()
        callback = MagicMock(return_value=True)
        element.add_message_callback(callback, MSG_ELEMENT_READY)
        await element.mount()
        assert callback.call_count == 1

        await element.unmount()

        # Act — second mount: callback should no longer be registered
        await element.mount()

        # Assert
        assert callback.call_count == 1  # still 1, not called again

    async def test_callback_returning_false_false_stops_propagation_to_pipeline(
        self,
    ) -> None:
        # Arrange
        pipeline = Pipeline("test")
        element = ConcreteElement(pipeline=pipeline)
        callback = MagicMock(return_value=(False, False))
        element.add_message_callback(callback, MSG_ELEMENT_READY)

        # Act
        with patch.object(pipeline, "send_message") as mock_send:
            await element.mount()

        # Assert
        callback.assert_called_once()
        mock_send.assert_not_called()

    @pytest.mark.parametrize(
        ("result", "stays_registered"),
        [
            (None, True),
            (False, True),
            ((False, True), True),
            (True, False),
            ((True, True), False),
        ],
    )
    async def test_callback_result_controls_removal(
        self, result: bool | tuple[bool, bool] | None, stays_registered: bool
    ) -> None:
        # Arrange
        element = ConcreteElement()
        callback = MagicMock(return_value=result)
        element.add_message_callback(callback, MSG_ELEMENT_READY)
        await element.mount()
        await element.unmount()

        # Act — second mount sends the same message type again
        await element.mount()

        # Assert
        assert callback.call_count == (2 if stays_registered else 1)

    async def test_callback_returning_true_false_removes_and_stops_propagation(
        self,
    ) -> None:
        # Arrange
        pipeline = Pipeline("test")
        element = ConcreteElement(pipeline=pipeline)
        callback = MagicMock(return_value=(True, False))
        element.add_message_callback(callback, MSG_ELEMENT_READY)
        await element.mount()
        await element.unmount()

        # Act — second mount: callback should be gone and the message propagate
        with patch.object(pipeline, "send_message") as mock_send:
            await element.mount()

        # Assert
        callback.assert_called_once()
        mock_send.assert_called_once()

    async def test_callback_stopping_propagation_skips_later_callbacks(self) -> None:
        # Arrange
        element = ConcreteElement()
        first = MagicMock(return_value=(False, False))
        second = MagicMock(return_value=None)
        element.add_message_callback(first, MSG_ELEMENT_READY)
        element.add_message_callback(second, MSG_ELEMENT_READY)

        # Act
        await element.mount()

        # Assert
        first.assert_called_once()
        second.assert_not_called()

    async def test_callback_receives_element_and_message_with_origin(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        element = ConcreteElement(name="elem-1", pipeline=pipeline)
        callback = MagicMock(return_value=None)
        element.add_message_callback(callback, MSG_ELEMENT_READY)

        # Act
        await element.mount()

        # Assert
        callback.assert_called_once_with(
            element,
            Message(
                sender="elem-1",
                pipeline="test",
                type=MSG_ELEMENT_READY,
                params={},
            ),
        )

    async def test_callback_not_propagated_message_does_not_reach_pipeline_callback(
        self,
    ) -> None:
        # Arrange
        pipeline = Pipeline("test")
        element = ConcreteElement(pipeline=pipeline)
        element.add_message_callback(
            MagicMock(return_value=(False, False)), MSG_ELEMENT_READY
        )
        pipeline_callback = MagicMock(return_value=None)
        pipeline.add_message_callback(pipeline_callback, MSG_ELEMENT_READY)

        # Act
        await element.mount()

        # Assert
        pipeline_callback.assert_not_called()

    async def test_mount_failure_sends_error_message_with_exception(self) -> None:
        # Arrange
        element = FailingMountElement(name="elem-1")
        callback = MagicMock(return_value=None)
        element.add_message_callback(callback, MSG_ELEMENT_ERROR)

        # Act
        with pytest.raises(ValueError, match="mount failed"):
            await element.mount()

        # Assert
        callback.assert_called_once()
        _, msg = callback.call_args[0]
        assert msg.sender == "elem-1"
        assert isinstance(msg.params["ex"], ValueError)

    async def test_skipped_frame_sends_frame_skipped_message(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = SkippingProducer(pipeline=pipeline)
        consumer = ConcreteSink(source=producer, pipeline=pipeline)
        await consumer.mount()
        received: list[Message] = []
        pipeline.add_message_callback(
            lambda _, msg: received.append(msg), MSG_FRAME_SKIPPED
        )

        # Act
        await consumer.consume_frame()

        # Assert
        assert [m.params for m in received] == [{"frame": 0}]


# ---------------------------------------------------------------------------
# TestBaseProducer
# ---------------------------------------------------------------------------


class TestPressureCtx:
    def test_enter_sets_pressure_and_returns_context(self) -> None:
        producer = ConcreteProducer([])
        context = PressureCtx(producer)

        with context as result:
            assert result is context
            assert result.elem is producer
            assert producer.pressure is True

    def test_exit_clears_pressure(self) -> None:
        producer = ConcreteProducer([])

        with PressureCtx(producer):
            pass

        assert producer.pressure is False
        assert producer.state is ElementState.NULL

    @pytest.mark.parametrize(
        "error", [ValueError("failed"), FrameSkipped(7), asyncio.CancelledError()]
    )
    def test_exception_propagates_and_clears_pressure(
        self, error: BaseException
    ) -> None:
        producer = ConcreteProducer([])

        with pytest.raises(type(error)) as caught, PressureCtx(producer):
            raise error

        assert caught.value is error
        assert producer.pressure is False


class TestActivePressure:
    async def test_call_forwards_arguments_and_returns_result(self) -> None:
        producer = ConcreteProducer([])
        observed: list[tuple[BaseProducer[int], int, str, bool]] = []

        @active_pressure
        async def operation(
            subject: BaseProducer[int], value: int, *, suffix: str
        ) -> str:
            observed.append((subject, value, suffix, subject.pressure))
            return f"{value}{suffix}"

        result = await operation(producer, 42, suffix="!")

        assert result == "42!"
        assert observed == [(producer, 42, "!", True)]
        assert producer.pressure is False

    async def test_pressure_stays_active_across_await(self) -> None:
        producer = ConcreteProducer([])
        entered = asyncio.Event()
        release = asyncio.Event()
        observed: list[bool] = []

        @active_pressure
        async def operation(subject: BaseProducer[int]) -> None:
            entered.set()
            await release.wait()
            observed.append(subject.pressure)

        task = asyncio.create_task(operation(producer))
        try:
            await asyncio.wait_for(entered.wait(), timeout=1)

            assert producer.pressure is True
            assert not task.done()
            release.set()
            await asyncio.wait_for(task, timeout=1)
            assert observed == [True]
            assert producer.pressure is False
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @pytest.mark.parametrize("error", [ValueError("failed"), FrameSkipped(7)])
    async def test_exception_propagates_and_clears_pressure(
        self, error: BaseException
    ) -> None:
        producer = ConcreteProducer([])

        @active_pressure
        async def operation(subject: BaseProducer[int]) -> None:
            assert subject.pressure is True
            raise error

        with pytest.raises(type(error)) as caught:
            await operation(producer)

        assert caught.value is error
        assert producer.pressure is False

    async def test_cancelled_operation_clears_pressure(self) -> None:
        producer = ConcreteProducer([])
        entered = asyncio.Event()
        release = asyncio.Event()

        @active_pressure
        async def operation(subject: BaseProducer[int]) -> None:
            entered.set()
            await release.wait()

        task = asyncio.create_task(operation(producer))
        await asyncio.wait_for(entered.wait(), timeout=1)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()
        assert producer.pressure is False


class TestBaseProducer:
    def test_pressure_is_initially_false(self) -> None:
        producer = ConcreteProducer([])

        assert producer.pressure is False

    async def test_pressure_is_active_while_read_waits(self) -> None:
        producer = PressureProducer()
        await producer.mount()
        task = asyncio.create_task(anext(producer))
        try:
            await asyncio.wait_for(producer.reading.wait(), timeout=1)

            assert producer.pressure is True
            assert producer.observed_pressure == [True]
            assert not task.done()
            assert producer.state is ElementState.READY
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await producer.unmount()

    async def test_successful_read_clears_pressure(self) -> None:
        producer = PressureProducer()
        await producer.mount()
        producer.frames.put_nowait(42)
        try:
            result = await anext(producer)

            assert result == 42
            assert producer.observed_pressure == [True]
            assert producer.pressure is False
            assert producer.state is ElementState.READY
        finally:
            await producer.unmount()

    @pytest.mark.parametrize("error", [StopAsyncIteration(), ValueError("failed")])
    async def test_failed_read_clears_pressure(self, error: Exception) -> None:
        producer = PressureProducer()
        await producer.mount()
        producer.frames.put_nowait(error)

        with pytest.raises(type(error)) as caught:
            await anext(producer)

        assert caught.value is error
        assert producer.observed_pressure == [True]
        assert producer.pressure is False
        assert producer.state is (
            ElementState.NULL
            if isinstance(error, StopAsyncIteration)
            else ElementState.ERROR
        )

    async def test_read_without_mount_leaves_pressure_false(self) -> None:
        producer = ConcreteProducer([1])

        with pytest.raises(RuntimeError, match="Element is not ready"):
            await anext(producer)

        assert producer.pressure is False
        assert producer.state is ElementState.NULL

    async def test_skipped_frame_keeps_pressure_active_until_read_completes(
        self,
    ) -> None:
        producer = PressureProducer()
        await producer.mount()
        producer.frames.put_nowait(FrameSkipped(0))
        producer.frames.put_nowait(42)
        try:
            result = await anext(producer)

            assert result == 42
            assert producer.observed_pressure == [True, True]
            assert producer.pressure is False
            assert producer.state is ElementState.READY
        finally:
            await producer.unmount()

    async def test_cancelled_read_clears_pressure(self) -> None:
        producer = PressureProducer()
        await producer.mount()
        task = asyncio.create_task(anext(producer))
        await asyncio.wait_for(producer.reading.wait(), timeout=1)
        try:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            assert task.cancelled()
            assert producer.pressure is False
            assert producer.state is ElementState.READY
        finally:
            await producer.unmount()

    async def test_unmount_clears_pressure_and_cancels_pending_read(self) -> None:
        producer = PressureProducer()
        await producer.mount()
        task = asyncio.create_task(anext(producer))
        await asyncio.wait_for(producer.reading.wait(), timeout=1)

        await producer.unmount()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
        assert producer.pressure is False
        assert producer.state is ElementState.NULL

    async def test_mount_clears_pressure(self) -> None:
        producer = ConcreteProducer([])
        with PressureCtx(producer):
            await producer.mount()

            assert producer.pressure is False
            assert producer.state is ElementState.READY
        await producer.unmount()

    async def test_anext_returns_first_frame_when_ready(self) -> None:
        # Arrange
        producer = ConcreteProducer([42, 99])
        await producer.mount()

        # Act
        result = await producer.__anext__()

        # Assert
        assert result == 42
        assert producer.state == ElementState.READY

    async def test_anext_returns_frames_in_order(self) -> None:
        # Arrange
        producer = ConcreteProducer([10, 20, 30])
        await producer.mount()

        # Act
        first = await producer.__anext__()
        second = await producer.__anext__()

        # Assert
        assert first == 10
        assert second == 20

    def test_aiter_returns_self(self) -> None:
        # Arrange
        producer = ConcreteProducer([1])

        # Act / Assert
        assert aiter(producer) is producer

    def test_rshift_with_consumer_connects_and_returns_consumer(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        consumer = ConcreteSink(pipeline=pipeline)

        # Act
        result = producer >> consumer

        # Assert
        assert result is consumer
        assert consumer.source is producer

    def test_rshift_with_non_consumer_raises_type_error(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        element = ConcreteElement(pipeline=pipeline)

        # Act / Assert
        with pytest.raises(TypeError):
            _ = producer >> element  # type: ignore[operator]

    def test_rshift_with_consumer_from_different_pipeline_raises(self) -> None:
        # Arrange
        p1 = Pipeline("p1")
        p2 = Pipeline("p2")
        producer = ConcreteProducer([1], pipeline=p1)
        consumer = ConcreteSink(pipeline=p2)

        # Act / Assert
        with pytest.raises(
            RuntimeError, match="Cannot connect elements from different pipelines"
        ):
            _ = producer >> consumer

    def test_get_product_type_raises_without_generic_type_args(self) -> None:
        # Arrange
        producer = ConcreteProducer([1])

        # Act / Assert
        with pytest.raises(TypeError, match="Product type is not available at runtime"):
            producer.get_product_type()


# ---------------------------------------------------------------------------
# TestBaseConsumer
# ---------------------------------------------------------------------------


class TestBaseConsumerInit:
    def test_init_without_source_source_is_none(self) -> None:
        # Act
        consumer = ConcreteSink()

        # Assert
        assert consumer.source is None

    def test_init_with_source_stores_source(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)

        # Act
        consumer = ConcreteSink(source=producer, pipeline=pipeline)

        # Assert
        assert consumer.source is producer


class TestBaseConsumerSource:
    def test_source_setter_updates_source(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        consumer = ConcreteSink(pipeline=pipeline)

        # Act
        consumer.source = producer

        # Assert
        assert consumer.source is producer

    async def test_source_setter_raises_when_element_is_ready(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1, 2], pipeline=pipeline)
        consumer = ConcreteSink(source=producer, pipeline=pipeline)
        await consumer.mount()
        assert consumer.state == ElementState.READY

        other_producer = ConcreteProducer([3], pipeline=pipeline)

        # Act / Assert
        with pytest.raises(RuntimeError, match="Unmount before change source"):
            consumer.source = other_producer

        assert consumer.source is producer  # unchanged


class TestBaseConsumerMount:
    async def test_mount_also_mounts_source_and_both_become_ready(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1, 2], pipeline=pipeline)
        consumer = ConcreteSink(source=producer, pipeline=pipeline)

        # Act
        await consumer.mount()

        # Assert
        assert consumer.state == ElementState.READY
        assert producer.state == ElementState.READY

    async def test_unmount_also_unmounts_source_and_both_become_null(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1, 2], pipeline=pipeline)
        consumer = ConcreteSink(source=producer, pipeline=pipeline)
        await consumer.mount()

        # Act
        await consumer.unmount()

        # Assert
        assert consumer.state == ElementState.NULL
        assert producer.state == ElementState.NULL


class TestBaseConsumerConsumeFrame:
    async def test_consume_frame_returns_frame_from_source(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([42, 99], pipeline=pipeline)
        consumer = ConcreteSink(source=producer, pipeline=pipeline)
        await consumer.mount()

        # Act
        frame = await consumer.consume_frame()

        # Assert
        assert frame == 42

    async def test_consume_frame_skips_frame_skipped_and_returns_next(self) -> None:
        # Arrange — source raises FrameSkipped on first call, then returns 42
        pipeline = Pipeline("test")
        producer = SkippingProducer(pipeline=pipeline)
        consumer = ConcreteSink(source=producer, pipeline=pipeline)
        await consumer.mount()

        # Act
        frame = await consumer.consume_frame()

        # Assert
        assert frame == 42


class TestBaseConsumerOperators:
    def test_lshift_connects_producer_and_returns_consumer(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        consumer = ConcreteSink(pipeline=pipeline)

        # Act
        result = consumer << producer

        # Assert
        assert result is consumer
        assert consumer.source is producer

    def test_lshift_parenthesised_chain_connects_each_pair(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        stage = ConcreteStage(pipeline=pipeline)
        sink = StrSink(pipeline=pipeline)

        # Act
        result = sink << (stage << producer)

        # Assert
        assert result is sink
        assert stage.source is producer
        assert sink.source is stage

    def test_rshift_chain_connects_each_pair(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        stage = ConcreteStage(pipeline=pipeline)
        sink = StrSink(pipeline=pipeline)

        # Act
        result = producer >> stage >> sink

        # Assert
        assert result is sink
        assert stage.source is producer
        assert sink.source is stage

    def test_shift_between_stages_of_same_type(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1], pipeline=pipeline)
        first = IntStage(pipeline=pipeline)
        second = IntStage(pipeline=pipeline)
        third = IntStage(pipeline=pipeline)

        # Act
        _ = producer >> first >> second
        _ = third << second

        # Assert
        assert first.source is producer
        assert second.source is first
        assert third.source is second

    def test_lshift_with_different_pipeline_raises(self) -> None:
        # Arrange
        p1 = Pipeline("p1")
        p2 = Pipeline("p2")
        producer = ConcreteProducer([1], pipeline=p1)
        consumer = ConcreteSink(pipeline=p2)

        # Act / Assert
        with pytest.raises(
            RuntimeError, match="Cannot connect elements from different pipelines"
        ):
            _ = consumer << producer

    def test_lshift_with_non_producer_raises_type_error(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        consumer = ConcreteSink(pipeline=pipeline)
        element = ConcreteElement(pipeline=pipeline)

        # Act / Assert
        with pytest.raises(TypeError):
            _ = consumer << element  # type: ignore[operator]


# ---------------------------------------------------------------------------
# TestBaseParallelProducer
# ---------------------------------------------------------------------------


class TestBaseParallelProducer:
    def test_pressure_is_initially_false(self) -> None:
        producer = ConcreteParallelProducer([])

        assert producer.pressure is False
        assert producer.state is ElementState.NULL

    @pytest.mark.parametrize("reader_count", [1, 3, 5])
    async def test_concurrent_readers_receive_each_frame_once(
        self, reader_count: int
    ) -> None:
        frames = list(range(reader_count))
        producer = ConcreteParallelProducer(frames)
        try:
            result = await asyncio.wait_for(
                asyncio.gather(*(anext(producer) for _ in frames)), timeout=1
            )

            # Waiting readers are held in a set; assignment order is unspecified.
            assert sorted(result) == frames
            assert producer.state is ElementState.READY
        finally:
            await producer.unmount()

    async def test_concurrent_readers_share_one_frame_reader(self) -> None:
        producer = ControlledParallelProducer()
        for frame in [10, 20, 30]:
            producer.frames.put_nowait(frame)
        try:
            result = await asyncio.wait_for(
                asyncio.gather(*(anext(producer) for _ in range(3))), timeout=1
            )

            assert sorted(result) == [10, 20, 30]
            assert producer.calls == 3
            assert producer.max_active_reads == 1
            assert producer.active_reads == 0
            assert producer.observed_pressure == [True, True, True]
        finally:
            await producer.unmount()

    async def test_read_does_not_fetch_unrequested_frame(self) -> None:
        producer = ControlledParallelProducer()
        producer.frames.put_nowait(42)
        producer.frames.put_nowait(99)
        try:
            result = await asyncio.wait_for(anext(producer), timeout=1)

            assert result == 42
            assert producer.calls == 1
            assert producer.frames.qsize() == 1
            assert producer.active_reads == 0
        finally:
            await producer.unmount()

    async def test_pressure_is_active_while_read_waits(self) -> None:
        producer = ControlledParallelProducer()
        task = asyncio.create_task(anext(producer))
        try:
            await asyncio.wait_for(producer.reading.wait(), timeout=1)

            assert producer.pressure is True
            assert producer.observed_pressure == [True]
            assert not task.done()
            assert producer.state is ElementState.READY
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await producer.unmount()

    async def test_successful_read_clears_pressure_when_no_readers_remain(self) -> None:
        producer = ControlledParallelProducer()
        producer.frames.put_nowait(42)
        try:
            result = await asyncio.wait_for(anext(producer), timeout=1)

            assert result == 42
            assert producer.pressure is False
            assert producer.state is ElementState.READY
        finally:
            await producer.unmount()

    @pytest.mark.parametrize("reader_count", [1, 3])
    async def test_exhaustion_finishes_all_waiting_readers_and_unmounts(
        self, reader_count: int
    ) -> None:
        producer = ConcreteParallelProducer([])
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *(anext(producer) for _ in range(reader_count)),
                    return_exceptions=True,
                ),
                timeout=1,
            )

            assert len(results) == reader_count
            assert all(isinstance(result, StopAsyncIteration) for result in results)
            assert producer.state is ElementState.NULL
            assert producer.pressure is False
        finally:
            await producer.unmount()

    async def test_skipped_frame_emits_message_and_keeps_reader_waiting(self) -> None:
        producer = ControlledParallelProducer()
        producer.frames.put_nowait(FrameSkipped(0))
        producer.frames.put_nowait(42)
        received: list[Message] = []
        producer.add_message_callback(
            lambda _, message: received.append(message), MSG_FRAME_SKIPPED
        )
        try:
            result = await asyncio.wait_for(anext(producer), timeout=1)

            assert result == 42
            assert producer.calls == 2
            assert producer.observed_pressure == [True, True]
            assert [message.params for message in received] == [{"frame": 0}]
            assert producer.state is ElementState.READY
        finally:
            await producer.unmount()

    async def test_cancelling_one_reader_keeps_other_reader_active(self) -> None:
        producer = ControlledParallelProducer()
        cancelled = asyncio.create_task(anext(producer))
        remaining = asyncio.create_task(anext(producer))
        await asyncio.wait_for(producer.reading.wait(), timeout=1)
        try:
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):
                await cancelled

            assert cancelled.cancelled()
            assert not remaining.done()
            assert producer.pressure is True
            assert not producer.interrupted.is_set()
            assert producer.state is ElementState.READY
        finally:
            remaining.cancel()
            await asyncio.gather(cancelled, remaining, return_exceptions=True)
            await producer.unmount()

    async def test_cancelling_last_reader_stops_frame_read_and_clears_pressure(
        self,
    ) -> None:
        producer = ControlledParallelProducer()
        task = asyncio.create_task(anext(producer))
        await asyncio.wait_for(producer.reading.wait(), timeout=1)
        try:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(producer.interrupted.wait(), timeout=1)

            assert task.cancelled()
            assert producer.active_reads == 0
            assert producer.pressure is False
        finally:
            await producer.unmount()

    async def test_unmount_cancels_all_waiting_readers(self) -> None:
        producer = ControlledParallelProducer()
        tasks = [asyncio.create_task(anext(producer)) for _ in range(3)]
        await asyncio.wait_for(producer.reading.wait(), timeout=1)

        await producer.unmount()

        await asyncio.gather(*tasks, return_exceptions=True)
        assert all(task.cancelled() for task in tasks)
        assert producer.state is ElementState.NULL
        assert producer.pressure is False
        assert producer.active_reads == 0
        assert producer.interrupted.is_set()

    @pytest.mark.parametrize("reader_count", [1, 3])
    async def test_frame_error_reaches_all_waiting_readers(
        self, reader_count: int
    ) -> None:
        producer = ControlledParallelProducer()
        error = ValueError("frame failed")
        producer.frames.put_nowait(error)

        results = await asyncio.wait_for(
            asyncio.gather(
                *(anext(producer) for _ in range(reader_count)),
                return_exceptions=True,
            ),
            timeout=1,
        )

        assert len(results) == reader_count
        assert all(result is error for result in results)
        assert producer.state is ElementState.ERROR
        assert producer.pressure is False
        assert producer.calls == 1
        assert producer.active_reads == 0

    async def test_new_request_restarts_worker_after_last_reader_cancellation(
        self,
    ) -> None:
        producer = ControlledParallelProducer()
        first = asyncio.create_task(anext(producer))
        await asyncio.wait_for(producer.reading.wait(), timeout=1)
        first.cancel()
        replacement = asyncio.create_task(anext(producer))
        producer.frames.put_nowait(42)
        try:
            result = await asyncio.wait_for(replacement, timeout=1)

            assert result == 42
            assert first.cancelled()
            assert producer.max_active_reads == 1
            assert producer.pressure is False
            assert producer.state is ElementState.READY
        finally:
            first.cancel()
            replacement.cancel()
            await asyncio.gather(first, replacement, return_exceptions=True)
            await producer.unmount()

    def test_frame_error_reaches_waiting_reader_without_stalling(self) -> None:
        # Bound this scenario in another process: an error-loop regression can
        # starve the event loop, preventing asyncio.wait_for from firing.
        scenario = dedent(
            """
            import asyncio
            from tests.test_base import ControlledParallelProducer

            async def main() -> None:
                producer = ControlledParallelProducer()
                producer.frames.put_nowait(ValueError("frame failed"))
                try:
                    await anext(producer)
                except ValueError as error:
                    assert str(error) == "frame failed"
                else:
                    raise AssertionError("reader did not receive the frame error")

            asyncio.run(main())
            """
        )

        result = subprocess.run(
            [sys.executable, "-c", scenario],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )

        assert result.returncode == 0, result.stderr

    async def test_anext_returns_frame(self) -> None:
        # Arrange
        producer = ConcreteParallelProducer([42])

        # Act
        result = await producer.__anext__()

        # Assert
        assert result == 42

    def test_aiter_returns_self(self) -> None:
        # Arrange
        producer = ConcreteParallelProducer([1])

        # Act / Assert
        assert aiter(producer) is producer


# ---------------------------------------------------------------------------
# TestBaseFullSink
# ---------------------------------------------------------------------------


class TestBaseFullSink:
    async def test_consume_all_processes_all_frames(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1, 2, 3], pipeline=pipeline)
        sink = ConcreteFullSink(source=producer, pipeline=pipeline)
        await sink.mount()

        async def run() -> None:
            await sink.consume_all()

        # Act — inner task: stream exhaustion cancels the consumer task;
        # running in a separate task avoids cancelling the test task.
        task = asyncio.create_task(run())
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Assert
        assert sink.processed == [1, 2, 3]

    async def test_consume_all_processes_falsy_frames(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([0, 1, 0], pipeline=pipeline)
        sink = ConcreteFullSink(source=producer, pipeline=pipeline)

        # Act
        await sink.consume_all()

        # Assert
        assert sink.processed == [0, 1, 0]
        assert sink.state is ElementState.NULL


# ---------------------------------------------------------------------------
# TestBaseStage
# ---------------------------------------------------------------------------


class TestBaseStage:
    async def test_anext_transforms_frame_when_mounted(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([42], pipeline=pipeline)
        stage = ConcreteStage(source=producer, pipeline=pipeline)
        await stage.mount()

        # Act
        result = await stage.__anext__()

        # Assert
        assert result == "42"
        assert stage.state == ElementState.READY
        assert producer.state == ElementState.READY

    async def test_mount_mounts_both_stage_and_source(self) -> None:
        # Arrange
        pipeline = Pipeline("test")
        producer = ConcreteProducer([1, 2], pipeline=pipeline)
        stage = ConcreteStage(source=producer, pipeline=pipeline)

        # Act
        await stage.mount()

        # Assert
        assert stage.state == ElementState.READY
        assert producer.state == ElementState.READY

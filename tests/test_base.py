import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pyrill.base import (
    BUS_MSG_ELEMENT_NULL,
    BUS_MSG_ELEMENT_READY,
    BaseElement,
    BaseFullSink,
    BaseParallelProducer,
    BaseProducer,
    BaseSink,
    BaseStage,
    ElementState,
    EndOfStream,
    FrameSkipped,
    Message,
    Pipeline,
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
# TestEndOfStream
# ---------------------------------------------------------------------------


class TestEndOfStream:
    def test_is_stop_async_iteration(self) -> None:
        # Act
        ex = EndOfStream()

        # Assert
        assert isinstance(ex, StopAsyncIteration)


# ---------------------------------------------------------------------------
# TestMessage
# ---------------------------------------------------------------------------


class TestMessage:
    def test_stores_sender_type_and_params(self) -> None:
        # Act
        msg = Message(sender="elem-1", type="some-event", params={"key": "val"})

        # Assert
        assert msg.sender == "elem-1"
        assert msg.type == "some-event"
        assert msg.params == {"key": "val"}

    def test_params_defaults_to_empty_dict(self) -> None:
        # Act
        msg = Message(sender="elem-1", type="some-event")

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
        assert any(m.type == BUS_MSG_ELEMENT_READY for m in messages)

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
        assert any(m.type == BUS_MSG_ELEMENT_NULL for m in messages)

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
        element.add_message_callback(callback, BUS_MSG_ELEMENT_READY)

        # Act
        await element.mount()

        # Assert
        callback.assert_called_once()
        _, msg = callback.call_args[0]
        assert msg.type == BUS_MSG_ELEMENT_READY

    async def test_callback_not_called_for_different_message_type(self) -> None:
        # Arrange
        element = ConcreteElement()
        callback = MagicMock(return_value=None)
        element.add_message_callback(callback, BUS_MSG_ELEMENT_NULL)

        # Act — mount sends BUS_MSG_ELEMENT_READY, not BUS_MSG_ELEMENT_NULL
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
        element.add_message_callback(callback, BUS_MSG_ELEMENT_READY)
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
        element.add_message_callback(callback, BUS_MSG_ELEMENT_READY)

        # Act
        with patch.object(pipeline, "send_message") as mock_send:
            await element.mount()

        # Assert
        callback.assert_called_once()
        mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# TestBaseProducer
# ---------------------------------------------------------------------------


class TestBaseProducer:
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

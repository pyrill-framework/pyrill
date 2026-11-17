from abc import ABC, abstractmethod
from asyncio import Condition, TimerHandle
from collections.abc import Callable, Hashable
from contextlib import suppress
from datetime import timedelta
from itertools import chain
from typing import Any, cast

from pyrill.base import (
    BaseDecoupledStage,
    BaseProducer,
    ElementState,
    FrameSkipped,
    Message,
    MessageBrokerMixin,
)
from pyrill.queues import QueueSource


class WindowQueue[Source_co](QueueSource[Source_co]):
    """
    Queue source that reports pressure while waiting for the next frame.

    The pressure flag is active during a queue read and cleared when it returns,
    raises or is cancelled. Frames are consumed in insertion order. Mount the
    source before pushing frames and call :meth:`end_of_stream` to close it.
    Connect the source to a sink to consume its frames.

    :param queue_size: Maximum number of queued frames; ``0`` means unlimited,
        the default. A writer waits while the queue is full.

    Input and output::

        pushed: 1, 2, StopAsyncIteration()
        output: 1, 2

    Usage:

    .. code-block:: python

        from pyrill.accumulators import ListAcc
        from pyrill.sinks import Last
        from pyrill.windows import WindowQueue

        window = WindowQueue[int]()

        await window.mount()
        await window.push_frame(1)
        await window.push_frame(2)
        await window.end_of_stream()

        sink = window >> ListAcc[int]() >> Last[list[int]]()
        frames = await sink.get_frame()
        # [1, 2]
    """


class BaseWindow[Source_co](
    BaseDecoupledStage[Source_co, BaseProducer[Source_co]], ABC
):
    """
    Abstract stage that partitions a stream into window producers.

    A background task fills the current window while downstream receives a
    :class:`~pyrill.base.BaseProducer` for its frames. Connect each producer to
    a sink to consume it. A final nonempty window is delivered when upstream
    ends; an empty source produces no windows. Unmounting stops the background
    task and closes the current window.

    Subclasses implement ``_create_window()`` to create the internal queue and
    define when to close it with ``_end_current_window()``. The
    ``_inner_start_current_window()`` and ``_inner_end_current_window()`` hooks
    allow per-window setup and cleanup. :class:`WindowSize` and
    :class:`WindowTime` provide size-based and time-based boundaries.

    Input and output::

        input:  1, 2, 3
        output: producer(1, 2, 3) (when the window closes at end of stream)

    Usage:

    .. code-block:: python

        from pyrill.accumulators import ListAcc
        from pyrill.base import BaseProducer
        from pyrill.mappers import Map
        from pyrill.sinks import Last
        from pyrill.sources import IterableSource
        from pyrill.windows import BaseWindow, WindowQueue

        class WholeStream(BaseWindow[int]):
            def _create_window(self) -> WindowQueue[int]:
                return WindowQueue[int]()

        async def collect_window(window: BaseProducer[int]) -> list[int]:
            sink = window >> ListAcc[int]() >> Last[list[int]]()
            return await sink.get_frame()

        sink = (
            IterableSource(source=[1, 2, 3])
            >> WholeStream()
            >> Map[BaseProducer[int], list[int]](map_func=collect_window)
            >> Last[list[int]]()
        )

        frames = await sink.get_frame()
        # [1, 2, 3]
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._current_window: WindowQueue[Source_co] | None = None
        self._condition = Condition()
        self._last_exception: BaseException | None = None
        self._window_has_frames = False

    async def _mount(self) -> None:
        self._last_exception = None
        self._window_has_frames = False
        await super()._mount()

    async def _unmount(self) -> None:
        # Stop writes before closing the window or unmounting the source.
        await self._stop_consumer()
        await self._end_current_window()
        async with self._condition:
            self._condition.notify_all()
        await super()._unmount()

    async def _push_frame(self, frame: Source_co | BaseException) -> None:
        if isinstance(frame, BaseException):
            self._last_exception = frame
            await self._end_current_window()
            async with self._condition:
                self._condition.notify_all()
            return

        async with self._condition:
            await self._condition.wait_for(
                lambda: (
                    self._current_window is not None or self.state != ElementState.READY
                )
            )
            self._ensure_ready()
            await self._push_to_window(frame)
            self._condition.notify_all()

    async def _push_to_window(self, frame: Source_co):
        assert self._current_window is not None
        await self._current_window.push_frame(frame)
        self._window_has_frames = True

    async def _end_current_window(self) -> None:
        async with self._condition:
            if self._current_window is not None:
                await self._current_window.end_of_stream()
                self._current_window = None
                self._inner_end_current_window()
            self._condition.notify_all()

    def _inner_end_current_window(self):
        "Implement on specific windows"

    async def _start_current_window(self) -> WindowQueue[Source_co]:
        # Reserve one extra slot for the end marker, even when no reader has
        # started draining a full window yet.
        window = self._create_window()
        await window.mount()
        async with self._condition:
            self._current_window = window
            self._window_has_frames = False
            self._inner_start_current_window()
            self._condition.notify_all()
        return window

    @abstractmethod
    def _create_window(self) -> WindowQueue[Source_co]:
        raise NotImplementedError

    def _inner_start_current_window(self):
        "Implement on specific windows"

    def _raise_last_exception(self) -> None:
        if self._last_exception is not None:
            raise self._last_exception

    async def _decoupled_next_frame(self) -> Source_co:
        raise NotImplementedError()

    async def _process_frame(self, frame: Source_co) -> BaseProducer[Source_co]:
        raise NotImplementedError()

    async def _next_frame(self) -> BaseProducer[Source_co]:
        async with self._condition:
            await self._condition.wait_for(
                lambda: (
                    self._current_window is None
                    or self._last_exception is not None
                    or self.state != ElementState.READY
                )
            )
        self._raise_last_exception()
        self._ensure_ready()

        window = await self._start_current_window()
        async with self._condition:
            await self._condition.wait_for(
                lambda: (
                    self._window_has_frames
                    or self._last_exception is not None
                    or self.state != ElementState.READY
                )
            )
            has_frames = self._window_has_frames

        # Keep the local reference: completing a window clears the producer's
        # current pointer, but its buffered frames must still reach the reader.
        if has_frames:
            self._ensure_ready()
            return window

        self.pipeline.create_orphan_task(window.unmount())
        self._raise_last_exception()
        self._ensure_ready()
        raise StopAsyncIteration()


class WindowSize[Source_co](BaseWindow[Source_co]):
    """
    Stage that groups consecutive frames into producers of a fixed size.

    Each output is a :class:`~pyrill.base.BaseProducer` supplying the frames of
    one window. Connect that producer to a sink to consume the window.
    Windows preserve input order and contain at most ``size`` frames. When upstream ends,
    a final partial window is emitted if it contains any frames; an empty source
    produces no windows.

    A background task fills the current window. Requesting the next window waits
    until the current one is complete, but completed windows can be read later.

    :param size: Maximum number of frames in each window. Must be positive.
    :raises ValueError: If ``size`` is zero or negative.

    Input and output::

        input:  1, 2, 3, 4, 5
        output: producer(1, 2), producer(3, 4), producer(5) (size=2)

    Usage:

    .. code-block:: python

        from pyrill.accumulators import ListAcc
        from pyrill.base import BaseProducer
        from pyrill.mappers import Map
        from pyrill.sinks import Last
        from pyrill.sources import IterableSource
        from pyrill.windows import WindowSize

        async def collect_window(window: BaseProducer[int]) -> list[int]:
            sink = window >> ListAcc[int]() >> Last[list[int]]()
            return await sink.get_frame()

        source = IterableSource(source=[1, 2, 3, 4, 5])
        sink = (
            source
            >> WindowSize[int](size=2)
            >> Map[BaseProducer[int], list[int]](map_func=collect_window)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )

        groups = await sink.get_frame()
        # [[1, 2], [3, 4], [5]]
    """

    def __init__(self, *args: Any, size: int, **kwargs: Any) -> None:
        if size <= 0:
            raise ValueError("Size must be positive")
        super().__init__(*args, **kwargs)

        self.size = size

    async def _mount(self) -> None:
        self._counter = 0
        await super()._mount()

    async def _push_frame(self, frame: Source_co | BaseException) -> None:
        await super()._push_frame(frame)

        if self._counter >= self.size:
            await self._end_current_window()

    async def _push_to_window(self, frame: Source_co):
        await super()._push_to_window(frame)
        self._counter += 1

    def _inner_end_current_window(self):
        self._counter = 0

    def _create_window(self) -> WindowQueue[Source_co]:
        return WindowQueue[Source_co](queue_size=self.size + 1)

    def _inner_start_current_window(self):
        self._counter = 0


class WindowTime[Source_co](BaseWindow[Source_co]):
    """
    Stage that groups consecutive frames into producers with a time limit.

    Each output is a :class:`~pyrill.base.BaseProducer` for one window; connect
    it to a sink to consume its frames. The timeout starts when the nonempty
    window is returned downstream. Once it expires, the current window closes
    and subsequent frames belong to a new window. Windows preserve input order
    and do not have a frame-count limit.

    Empty windows are not emitted while waiting for input. When upstream ends,
    any final nonempty window closes immediately, without waiting for its
    timeout. These are relative window deadlines rather than fixed intervals
    aligned to a wall clock.

    :param timeout: Duration for which an emitted window accepts more frames.
        Must be a positive :class:`~datetime.timedelta`.
    :raises ValueError: If ``timeout`` is zero or negative.

    Input and output::

        input:  1, 2, 3 (frames arrive farther apart than the timeout)
        output: producer(1), producer(2), producer(3)

    Usage:

    .. code-block:: python

        from datetime import timedelta

        from pyrill.accumulators import ListAcc
        from pyrill.base import BaseProducer
        from pyrill.mappers import Map
        from pyrill.sinks import Last
        from pyrill.sources import IterableSource
        from pyrill.time import Delayed
        from pyrill.windows import WindowTime

        async def collect_window(window: BaseProducer[int]) -> list[int]:
            sink = window >> ListAcc[int]() >> Last[list[int]]()
            return await sink.get_frame()

        sink = (
            IterableSource(source=[1, 2, 3])
            >> Delayed[int](delay=0.05)
            >> WindowTime[int](timeout=timedelta(milliseconds=10))
            >> Map[BaseProducer[int], list[int]](map_func=collect_window)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )

        groups = await sink.get_frame()
        # Typically [[1], [2], [3]]; boundaries depend on arrival and scheduling.
    """

    def __init__(self, *args: Any, timeout: timedelta, **kwargs: Any) -> None:
        if timeout.total_seconds() <= 0:
            raise ValueError("timeout must be positive")
        super().__init__(*args, **kwargs)

        self.timeout = timeout
        self._handler: TimerHandle | None = None

    async def _mount(self) -> None:
        self._invalidate_handler()
        await super()._mount()

    async def _unmount(self) -> None:
        self._invalidate_handler()
        await super()._unmount()

    def _invalidate_handler(self):
        if self._handler is None:
            return

        self._handler.cancel()
        self._handler = None

    def _inner_end_current_window(self):
        self._invalidate_handler()

    def _create_window(self) -> WindowQueue[Source_co]:
        return WindowQueue[Source_co]()

    async def _cut_window(self, window: WindowQueue[Source_co]):
        if self._current_window != window:
            return
        await self._end_current_window()

    async def _next_frame(self) -> BaseProducer[Source_co]:
        result = await super()._next_frame()

        self.pipeline.loop.call_later(
            self.timeout.total_seconds(),
            self.pipeline.create_orphan_task,
            self._cut_window(cast(WindowQueue[Source_co], result)),
        )

        return result


class GroupQueue[Source_co](QueueSource[Source_co]):
    """
    Queue source that exposes the key associated with a group of frames.

    :attr:`key` returns the hashable object supplied at construction. The
    property is read-only and retains its value across consumption, mounting,
    and unmounting. Frames are consumed in insertion order. :class:`GroupBy`
    creates these sources for the keys returned by its key builder.

    When feeding a source directly, mount it before calling :meth:`push_frame`
    and call :meth:`end_of_stream` to close it. Connect it to a sink to consume
    the buffered frames.

    :param key: Hashable key identifying this group. Values such as ``None``,
        ``0`` and an empty string are valid keys.
    :param queue_size: Maximum number of queued frames; ``0`` means unlimited,
        the default. A writer waits while the queue is full.

    Input and output::

        key:    "odd"
        pushed: 1, 3, StopAsyncIteration()
        output: 1, 3

    Usage:

    .. code-block:: python

        from pyrill.accumulators import ListAcc
        from pyrill.sinks import Last
        from pyrill.windows import GroupQueue

        group = GroupQueue[int](key="odd")
        await group.mount()
        await group.push_frame(1)
        await group.push_frame(3)
        await group.end_of_stream()

        sink = group >> ListAcc[int]() >> Last[list[int]]()
        frames = await sink.get_frame()
        # frames == [1, 3]
        # group.key == "odd"
    """

    def __init__(self, *args: Any, key: Hashable, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._key = key

    @property
    def key(self) -> Hashable:
        """
        Read-only key identifying this group, preserved across lifecycle changes.
        """
        return self._key


class GroupBy[Source_co](BaseDecoupledStage[Source_co, BaseProducer[Source_co]]):
    """
    Stage that routes frames with equal keys to the same group producer.

    ``key_builder(frame)`` computes a hashable key for each input frame. The
    stage emits a :class:`~pyrill.base.BaseProducer` when it first encounters a
    key, in first-seen key order. Each producer supplies the original frames
    for that key in input order, including frames separated by other keys.
    Connect group producers to sinks to consume them; their concrete types are
    implementation details. :class:`GroupQueue` sources expose their grouping
    key through :attr:`GroupQueue.key`.

    Groups close when upstream ends or the stage is unmounted. Buffered group
    frames can be consumed after the outer stream ends. Group buffers are
    unbounded, so retaining groups without consuming them retains their frames.
    If the key builder raises an exception, the frame is skipped, a warning is
    logged, and a :data:`~pyrill.base.MSG_FRAME_SKIPPED` message identifies the
    rejected frame. Other frames continue through the pipeline.

    :param key_builder: Synchronous function that receives a frame and returns
        its grouping key. Keys must be hashable.

    Input and output::

        input:  1, 2, 3, 4, 5
        output: producer(1, 3, 5), producer(2, 4) (key_builder=lambda x: x % 2)

    Usage:

    .. code-block:: python

        from pyrill.accumulators import ListAcc
        from pyrill.base import BaseProducer
        from pyrill.mappers import Map
        from pyrill.sinks import Last
        from pyrill.sources import IterableSource
        from pyrill.windows import GroupBy

        async def collect_group(group: BaseProducer[int]) -> list[int]:
            sink = group >> ListAcc[int]() >> Last[list[int]]()
            return await sink.get_frame()

        sink = (
            IterableSource(source=[1, 2, 3, 4, 5])
            >> GroupBy[int](key_builder=lambda frame: frame % 2)
            >> Map[BaseProducer[int], list[int]](map_func=collect_group)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )

        groups = await sink.get_frame()
        # [[1, 3, 5], [2, 4]]
    """

    def __init__(
        self, *args: Any, key_builder: Callable[[Source_co], Hashable], **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)

        self._key_builder = key_builder
        self._groups: dict[Hashable, GroupQueue[Source_co]] = {}
        self._groups_queue: QueueSource[GroupQueue[Source_co]] | None = None

        self._condition = Condition()

    async def _stop_all_groups(self):
        while len(self._groups):
            _, group = self._groups.popitem()
            with suppress(Exception):
                await group.end_of_stream()

        if self._groups_queue:
            with suppress(Exception):
                await self._groups_queue.end_of_stream()

    async def _mount(self) -> None:
        self._window_has_frames = False
        self._groups_queue = QueueSource()
        self._groups_queue.add_message_callback(self._proxy_messages)
        await self._groups_queue.mount()

        await super()._mount()

    def _proxy_messages(
        self, sender: MessageBrokerMixin, message: Message
    ) -> tuple[bool, bool]:
        self._send_message(self._build_message(message.type, **message.params))

        return False, False

    async def _unmount(self) -> None:
        # Stop writes before closing the window or unmounting the source.
        await self._stop_consumer()
        await self._stop_all_groups()
        if self._groups_queue:
            await self._groups_queue.unmount()
            self._groups_queue = None

        await super()._unmount()
        async with self._condition:
            self._condition.notify_all()

    async def _push_frame(self, frame: Source_co | BaseException) -> None:
        if isinstance(frame, BaseException):
            assert self._groups_queue is not None
            await self._groups_queue.push_frame(frame)
            await self._stop_all_groups()
            return

        async with self._condition:
            await self._condition.wait_for(
                lambda: (
                    any(
                        chain(
                            [g.pressure for g in self._groups.values()],
                            (self.pressure,),
                        )
                    )
                    or self.state != ElementState.READY
                )
            )
            self._ensure_ready()
            await self._push_to_group(frame)
            self._condition.notify_all()

    async def _push_to_group(self, frame: Source_co):
        try:
            key = self._key_builder(frame)
        except Exception as ex:
            self._logger.warning("Exception raise on key building", exc_info=ex)
            assert self._groups_queue is not None
            await self._groups_queue.push_frame(FrameSkipped(frame))
            return
        if key not in self._groups:
            group = GroupQueue[Source_co](key=key)
            self._groups[key] = group
            await group.mount()
            await group.push_frame(frame)

            assert self._groups_queue is not None
            await self._groups_queue.push_frame(group)
        else:
            try:
                await self._groups[key].push_frame(frame)
            except Exception as ex:
                self._logger.warning("Exception raise on pushing frame", exc_info=ex)
                self._groups.pop(key)
                assert self._groups_queue is not None
                await self._groups_queue.push_frame(FrameSkipped(frame))

    async def _decoupled_next_frame(self) -> Source_co:
        raise NotImplementedError()

    async def _process_frame(self, frame: Source_co) -> BaseProducer[Source_co]:
        raise NotImplementedError()

    async def _next_frame(self) -> BaseProducer[Source_co]:
        assert self._groups_queue is not None

        return await anext(self._groups_queue)

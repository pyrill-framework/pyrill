from asyncio import Condition
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, Self, cast

from pyrill.base import (
    BaseDecoupledStage,
    BaseElement,
    BaseProducer,
    ElementState,
)
from pyrill.queues import QueueSource


class PressureMixin(BaseElement):
    """
    Mixin that exposes whether an operation is applying pressure to an element.

    :attr:`pressure` starts as ``False`` and resets on mount and unmount.
    :class:`PressureCtx` and :func:`active_pressure` set it while an operation is
    active. The flag reports activity; it does not block reads or writes itself.
    :class:`WindowQueue` and :class:`WindowSize` use it while reading frames.

    Usage:

    .. code-block:: python

        from pyrill.windows import PressureCtx, PressureMixin

        element = PressureMixin()

        with PressureCtx(element):
            assert element.pressure

        assert not element.pressure
    """

    _pressure: bool = False

    @property
    def pressure(self) -> bool:
        """
        Whether a pressure context is currently active for this element.
        """
        return self._pressure

    async def _mount(self) -> None:
        self._pressure = False
        return await super()._mount()

    async def _unmount(self) -> None:
        self._pressure = False
        return await super()._unmount()


class PressureCtx:
    """
    Context manager that activates an element's pressure flag for a block.

    Entering sets :attr:`PressureMixin.pressure` to ``True`` and returns the
    context manager. Exiting sets it to ``False``, including when the block
    raises an exception or an awaiting task is cancelled. Exceptions propagate
    to the caller. The flag is not a counter: nested or overlapping contexts
    do not track their activity independently.

    :param elem: Element whose pressure flag is updated.

    Usage:

    .. code-block:: python

        from pyrill.windows import PressureCtx, PressureMixin

        element = PressureMixin()

        with PressureCtx(element) as context:
            assert context.elem is element
            assert element.pressure

        assert not element.pressure
    """

    def __init__(self, elem: PressureMixin) -> None:
        self.elem = elem

    def __enter__(self) -> Self:
        self.elem._pressure = True  # pyright: ignore[reportPrivateUsage]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.elem._pressure = False  # pyright: ignore[reportPrivateUsage]


def active_pressure[T: Callable[..., Awaitable[Any]]](func: T) -> T:
    """
    Decorate an asynchronous method to activate pressure while it runs.

    The method's first argument must be a :class:`PressureMixin` instance. The
    wrapper forwards arguments and returns the awaited result inside a
    :class:`PressureCtx`. Pressure remains active across awaits and is cleared
    on return, exception or cancellation.

    :param func: Asynchronous method to wrap.
    :return: A wrapper with the same callable type as the supplied method.

    Usage:

    .. code-block:: python

        from pyrill.windows import PressureMixin, active_pressure

        class Reader(PressureMixin):
            @active_pressure
            async def read(self) -> bool:
                return self.pressure

        reader = Reader()
        assert await reader.read()
        assert not reader.pressure
    """

    async def inner(self: PressureMixin, *args: Any, **kwargs: Any) -> Any:
        with PressureCtx(self):
            return await func(self, *args, **kwargs)

    return cast(T, inner)


class WindowQueue[Source_co](PressureMixin, QueueSource[Source_co]):
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

    @active_pressure
    async def _next_frame(self) -> Source_co:
        return await super()._next_frame()


class WindowSize[Source_co](
    PressureMixin, BaseDecoupledStage[Source_co, BaseProducer[Source_co]]
):
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
        self._counter = 0
        self._current_window: WindowQueue[Source_co] | None = None
        self._condition = Condition()
        self._last_exception: BaseException | None = None
        self._window_has_frames = False

    async def _mount(self) -> None:
        self._counter = 0
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
            window = self._current_window
            assert window is not None
            await window.push_frame(frame)
            self._counter += 1
            self._window_has_frames = True
            self._condition.notify_all()

        if self._counter >= self.size:
            await self._end_current_window()

    async def _end_current_window(self) -> None:
        async with self._condition:
            if self._current_window is not None:
                await self._current_window.end_of_stream()
                self._current_window = None
                self._counter = 0
            self._condition.notify_all()

    async def _start_current_window(self) -> WindowQueue[Source_co]:
        # Reserve one extra slot for the end marker, even when no reader has
        # started draining a full window yet.
        window = WindowQueue[Source_co](queue_size=self.size + 1)
        await window.mount()
        async with self._condition:
            self._current_window = window
            self._counter = 0
            self._window_has_frames = False
            self._condition.notify_all()
        return window

    def _raise_last_exception(self) -> None:
        if self._last_exception is not None:
            raise self._last_exception

    async def _decoupled_next_frame(self) -> Source_co:
        raise NotImplementedError()

    async def _process_frame(self, frame: Source_co) -> BaseProducer[Source_co]:
        raise NotImplementedError()

    @active_pressure
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

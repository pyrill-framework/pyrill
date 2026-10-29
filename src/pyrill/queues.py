from abc import ABC
from asyncio import Queue as AsyncQueue
from asyncio import QueueShutDown
from contextlib import suppress
from typing import Any

from .base import (
    BaseDecoupledStage,
    BaseProducer,
    BaseSource,
)

__all__ = [
    "BaseQueue",
    "Queue",
    "QueueSource",
]


class BaseQueue[Source_co](BaseProducer[Source_co], ABC):
    """
    Base class of elements that buffer frames in an :class:`asyncio.Queue`.

    A writer pushes frames and a reader consumes them. The queue is created on mount
    and shut down on unmount. Pushing an exception or ``StopAsyncIteration`` closes
    the queue for further writes (a later push raises :class:`RuntimeError`); the
    reader receives the frames already queued, then the exception or the end of the
    stream. :class:`Queue` and :class:`QueueSource` build on it.

    :param queue_size: Maximum number of frames in the queue; ``0`` means unlimited.
        When the queue is full, the writer waits.
    """

    _queue: AsyncQueue[Source_co | BaseException] | None = None

    def __init__(self, *args: Any, queue_size: int = 0, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._queue_size = queue_size
        self._open_queue = False

    async def _mount(self) -> None:
        self._queue = AsyncQueue(maxsize=self._queue_size)
        self._open_queue = True
        await super()._mount()

    async def _unmount(self) -> None:
        self._open_queue = False
        if self._queue is not None:
            self._queue.shutdown()
        await super()._unmount()

    async def _push_frame(self, frame: Source_co | BaseException) -> None:
        if not self._open_queue:
            raise RuntimeError("Stream already finished")
        if self._queue is None:
            raise RuntimeError("Queue not ready")

        if isinstance(frame, (Exception, StopAsyncIteration)):
            self._open_queue = False

        with suppress(QueueShutDown):
            await self._queue.put(frame)

    async def _next_frame(self) -> Source_co:
        if self._queue is None:
            raise StopAsyncIteration()

        try:
            frame = await self._queue.get()
        except QueueShutDown:
            raise StopAsyncIteration()

        if isinstance(frame, BaseException):
            raise frame
        return frame


class Queue[Source_co](BaseQueue[Source_co], BaseDecoupledStage[Source_co, Source_co]):
    """
    Stage that buffers frames in a queue, decoupling the upstream from the downstream.

    A background task reads the upstream as fast as the queue allows and the downstream
    reads from the queue at its own pace. Frames are passed unchanged and in order. When
    the upstream ends or fails, the queued frames are delivered first, then the end of
    the stream or the exception.

    :param queue_size: Maximum number of buffered frames; ``0`` means unlimited, the
        default. With a limit, the upstream waits while the queue is full.

    Input and output::

        input:  1, 2, 3
        output: 1, 2, 3

    Usage:

    .. code-block:: python

        from pyrill.queues import Queue
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        stage = source >> Queue(queue_size=2)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3]
    """

    async def _process_frame(self, frame: Source_co) -> Source_co:
        return frame

    async def _decoupled_next_frame(self) -> Source_co:
        return await super()._next_frame()


class QueueSource[Source_co](BaseQueue[Source_co], BaseSource[Source_co]):
    """
    Source that emits the frames pushed into it by application code.

    Call :meth:`push_frame` to add frames and push ``StopAsyncIteration()`` to end the
    stream. Pushing an exception delivers it to the reader and closes the stream. The
    source must be mounted before pushing; pushing after the stream is closed raises
    :class:`RuntimeError`.

    :param queue_size: Maximum number of queued frames; ``0`` means unlimited, the
        default. With a limit, :meth:`push_frame` waits while the queue is full.

    Input and output::

        pushed: 1, 2, StopAsyncIteration()
        output: 1, 2

    Usage:

    .. code-block:: python

        from pyrill.queues import QueueSource

        source = QueueSource[int]()

        await source.mount()
        await source.push_frame(1)
        await source.push_frame(2)
        await source.push_frame(StopAsyncIteration())

        frames = [frame async for frame in source]
        # [1, 2]
    """

    async def push_frame(self, frame: Source_co | BaseException) -> None:
        """
        Add a frame, an exception or ``StopAsyncIteration()`` to the queue.
        """
        await super()._push_frame(frame)

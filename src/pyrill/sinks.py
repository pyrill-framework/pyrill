from abc import ABC, abstractmethod
from typing import cast

from .base import BaseFullSink, BaseSink

__all__ = [
    "BaseOneFrameSink",
    "BlackHole",
    "First",
    "Last",
]


class BaseOneFrameSink[Sink_co](BaseSink[Sink_co], ABC):
    """
    Base class of sinks that reduce a stream to a single result.

    :meth:`get_frame` mounts the sink, awaits ``_get_frame`` and always unmounts it. A
    subclass implements ``async def _get_frame(self)``, reading upstream with
    :meth:`consume_frame`. The built-in subclasses are :class:`First` and :class:`Last`.

    Usage:

    .. code-block:: python

        from pyrill.sinks import BaseOneFrameSink
        from pyrill.sources import IterableSource


        class Total(BaseOneFrameSink[int]):
            async def _get_frame(self) -> int:
                total = 0
                try:
                    while True:
                        total += await self.consume_frame()
                except StopAsyncIteration:
                    return total


        sink = IterableSource(source=[1, 2, 3]) >> Total()

        total = await sink.get_frame()
        # 6
    """

    async def get_frame(self) -> Sink_co:
        """
        Mount the sink, return its single result and unmount.
        """
        await self.mount()
        try:
            return await self._get_frame()
        finally:
            await self.unmount()

    @abstractmethod
    async def _get_frame(self) -> Sink_co:
        raise NotImplementedError()


class First[Sink_co](BaseOneFrameSink[Sink_co]):
    """
    Sink that returns the first frame of the stream.

    It reads one frame and stops, then unmounts, which also unmounts the upstream
    elements. It raises :class:`ValueError` if the stream is empty.

    Input and output::

        input:  "a", "b", "c"
        output: "a"

    Usage:

    .. code-block:: python

        from pyrill.sinks import First
        from pyrill.sources import IterableSource

        sink = IterableSource(source=["a", "b", "c"]) >> First()

        first = await sink.get_frame()
        # "a"
    """

    async def _get_frame(self) -> Sink_co:
        try:
            return await self.consume_frame()
        except StopAsyncIteration:
            raise ValueError("No frames consumed")


class Last[Sink_co](BaseOneFrameSink[Sink_co]):
    """
    Sink that returns the last frame of the stream.

    It consumes the whole stream and returns the final frame. It raises
    :class:`ValueError` if the stream is empty.

    Input and output::

        input:  "a", "b", "c"
        output: "c"

    Usage:

    .. code-block:: python

        from pyrill.sinks import Last
        from pyrill.sources import IterableSource

        sink = IterableSource(source=["a", "b", "c"]) >> Last()

        last = await sink.get_frame()
        # "c"
    """

    EMPTY = object()

    async def _get_frame(self) -> Sink_co:
        last_frame: Sink_co | object = self.EMPTY
        while True:
            try:
                last_frame = await self.consume_frame()
            except StopAsyncIteration:
                if last_frame is self.EMPTY:
                    raise ValueError("No frames consumed")
                return cast(Sink_co, last_frame)


class BlackHole[Sink_co](BaseFullSink[Sink_co]):
    """
    Sink that consumes and discards every frame.

    Use it to drive a pipeline for its side effects, for example a stage that writes
    to a file, when the frames themselves are not needed. :meth:`consume_all` returns
    when the stream ends.

    Usage:

    .. code-block:: python

        from pyrill.sinks import BlackHole
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        sink = source >> BlackHole()

        await sink.consume_all()
        state = str(sink.state)
        # "null"
    """

    async def _process_frame(self, frame: Sink_co) -> None:
        # No operation, just consume the frame
        pass

from abc import ABC, abstractmethod
from asyncio import Condition
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from .base import (
    MSG_FRAME_SKIPPED,
    BaseProducer,
    BaseStage,
    FrameSkipped,
)

__all__ = [
    "BaseChunksFirstSeparatorProducer",
    "BaseChunksFirstSeparatorSource",
    "BaseChunksFirstSeparatorStage",
    "BaseChunksSeparatorProducer",
    "BaseChunksSeparatorSource",
    "BaseChunksSeparatorStage",
    "BaseChunksSlowStartProducer",
    "BaseChunksSlowStartSource",
    "BaseChunksSlowStartStage",
    "BaseDataAccumulatorStage",
    "BaseDataChunkProducer",
    "BaseDataChunkSource",
    "BaseDataChunkStage",
    "BaseSizedChunksProducer",
    "BaseSizedChunksSource",
    "BaseSizedChunksStage",
    "SkipUntil",
]


class BaseDataChunkProducer[AnyStr: (str, bytes)](BaseProducer[AnyStr], ABC):
    """
    Base class of every element that buffers data and re-emits it as chunks.

    It is shared by :class:`BaseDataChunkSource` (frames pushed by the caller) and
    :class:`BaseDataChunkStage` (frames read from the upstream). It keeps an internal
    ``_buffer`` that is emptied on mount and on unmount, and the optional
    ``max_buffer_size`` limit. Concrete classes combine it with a mixin, such as
    :class:`pyrill.strings.StringChunksMixin`, that sets ``_buffer`` to the empty
    value of the type (``""`` or ``b""``).

    Subclasses decide how to chunk by overriding two methods:

    * ``can_consume_buffer()``: return ``True`` when a chunk can be emitted. The
      default is ``True`` while the buffer is not empty.
    * ``_consume_buffer(closing)``: remove a chunk from ``_buffer`` and return it.
      ``closing`` is ``True`` when the stream has ended and the buffer is being
      flushed. The default returns the whole buffer.

    :param max_buffer_size: Maximum number of items (characters or bytes) that the
        buffer may hold. Exceeding it raises :class:`OverflowError`. ``0`` disables the
        limit. Defaults to ``0``.

    Minimal subclass that emits fixed groups of three characters::

        input:  "ab", "cdefg", "h"
        output: "abc", "def", "gh"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseDataChunkStage
        from pyrill.sources import IterableSource


        class Triplets(BaseDataChunkStage[str]):
            _buffer: str = ""

            def can_consume_buffer(self) -> bool:
                return len(self._buffer) >= 3

            def _consume_buffer(self, closing: bool = False) -> str:
                chunk, self._buffer = self._buffer[:3], self._buffer[3:]
                return chunk


        source = IterableSource(source=["ab", "cdefg", "h"])
        stage = source >> Triplets()

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abc", "def", "gh"]
    """

    _buffer: AnyStr

    def __init__(self, *args: Any, max_buffer_size: int = 0, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.max_buffer_size = max_buffer_size

    async def _mount(self) -> None:
        self._buffer = self._buffer[:0]
        await super()._mount()

    async def _unmount(self) -> None:
        self._buffer = self._buffer[:0]
        await super()._unmount()

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the buffer holds data that can be emitted as a chunk.
        """
        return len(self._buffer) > 0

    def _consume_buffer(self, closing: bool = False) -> AnyStr:
        result = self._buffer
        self._buffer = self._buffer[:0]
        return result


class BaseDataChunkSource[AnyStr: (str, bytes)](BaseDataChunkProducer[AnyStr], ABC):
    """
    Base class of sources that re-chunk data pushed by the caller.

    Frames are fed with :meth:`push_frame` and read by iterating the source. Call
    :meth:`end_of_stream` once all data has been pushed. Pushing an exception
    instance stores it as the last error and closes the input: it is raised to the
    reader after the buffered data has been consumed.

    When the input is closed, ``can_consume_buffer()`` returns ``True`` so that the
    reader flushes the remaining buffer and then receives the end of the stream. The
    extension contract is the one of :class:`BaseDataChunkProducer`: a subclass sets
    ``_buffer`` (through a mixin) and overrides ``can_consume_buffer()`` and
    ``_consume_buffer()``.

    :param max_buffer_size: Maximum number of items that the buffer may hold. When it
        is exceeded the buffer is cleared, :class:`OverflowError` is raised to the
        caller of :meth:`push_frame` and the same error is raised to the reader.
        ``0`` disables the limit. Defaults to ``0``.

    Input and output::

        pushed:  "ab", "cd", <end of stream>
        emitted: "abcd"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseDataChunkSource


        class Joiner(BaseDataChunkSource[str]):
            _buffer: str = ""


        source = Joiner()
        await source.mount()

        await source.push_frame("ab")
        await source.push_frame("cd")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["abcd"]
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._open_buffer = False
        self._condition = Condition()

    async def _mount(self) -> None:
        self._open_buffer = True
        await super()._mount()

    async def _unmount(self) -> None:
        self._open_buffer = False

        async with self._condition:
            self._condition.notify_all()

        await super()._unmount()

    async def push_frame(self, frame: AnyStr | Exception) -> None:
        """
        Append a frame to the buffer, or close the input when given an exception.

        Raises :class:`RuntimeError` when the source is not mounted or the stream has
        already ended. A :class:`StopAsyncIteration` instance ends the stream.
        """
        if not self._open_buffer:
            raise RuntimeError("Stream already finished or not mounted")

        if isinstance(frame, StopAsyncIteration):
            self._open_buffer = False
        elif isinstance(frame, Exception):
            self._last_error = frame
            self._open_buffer = False
        else:
            self._buffer += frame
            if self.max_buffer_size and len(self._buffer) > self.max_buffer_size:
                ex = OverflowError("Buffer exceeded max_buffer_size")
                self._buffer = self._buffer[:0]
                await self.push_frame(ex)
                raise ex

        await self._notify()

    async def end_of_stream(self) -> None:
        """
        Signal that no more data will be pushed, so the buffer is flushed and closed.
        """
        return await self.push_frame(StopAsyncIteration())

    async def _notify(self) -> None:
        async with self._condition:
            self._condition.notify()

    async def _next_frame(self) -> AnyStr:
        while True:
            try:
                return await self._next_chunk()
            except StopAsyncIteration:
                if self._last_error:
                    raise self._last_error
                raise
            except FrameSkipped:
                pass

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when a chunk can be emitted or the input has been closed.
        """
        return not self._open_buffer or super().can_consume_buffer()

    async def _next_chunk(self) -> AnyStr:
        async with self._condition:
            await self._condition.wait_for(self.can_consume_buffer)
            if self._buffer:
                return self._consume_buffer(not self._open_buffer)
            if self._last_error:
                raise self._last_error
            raise StopAsyncIteration()


class BaseDataChunkStage[AnyStr: (str, bytes)](
    BaseDataChunkProducer[AnyStr], BaseStage[AnyStr, AnyStr], ABC
):
    """
    Base class of stages that re-chunk the data of their upstream.

    Each incoming frame is appended to the buffer. While ``can_consume_buffer()``
    returns ``True`` the stage emits chunks taken with ``_consume_buffer()`` before it
    reads another frame. When the upstream ends, the remaining buffer is flushed
    (``_consume_buffer(True)`` is called until the buffer is empty) and the stream
    ends. The extension contract is the one of :class:`BaseDataChunkProducer`.

    :param max_buffer_size: Maximum number of items that the buffer may hold.
        Exceeding it raises :class:`OverflowError`. ``0`` disables the limit.
        Defaults to ``0``.

    Input and output of the default behaviour, which emits the whole buffer::

        input:  "ab", "cd"
        output: "ab", "cd"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseDataChunkStage
        from pyrill.sources import IterableSource


        class Passthrough(BaseDataChunkStage[str]):
            _buffer: str = ""


        source = IterableSource(source=["ab", "cd"])
        stage = source >> Passthrough()

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["ab", "cd"]
    """

    async def consume_frame(self) -> AnyStr:
        """
        Return the next chunk, reading from the upstream until one can be emitted.
        """
        while True:
            if self.can_consume_buffer():
                return self._consume_buffer()
            try:
                self._buffer += await super().consume_frame()  # type: ignore
                if self.max_buffer_size and len(self._buffer) > self.max_buffer_size:
                    raise OverflowError("Buffer exceeded max_buffer_size")
            except StopAsyncIteration:
                if self._buffer:
                    return self._consume_buffer(True)
                raise

    async def _process_frame(self, frame: AnyStr) -> AnyStr:
        return frame


class BaseSizedChunksProducer[AnyStr: (str, bytes)](BaseDataChunkProducer[AnyStr], ABC):
    """
    Base class of elements that emit chunks of bounded size.

    A chunk is emitted as soon as the buffer holds more than ``min_size`` items, and
    each chunk holds at most ``max_size`` items. When the stream ends, the remaining
    items are flushed as a final, possibly smaller, chunk. Concrete classes are built
    with :class:`BaseSizedChunksSource` or :class:`BaseSizedChunksStage` plus a mixin
    that sets ``_buffer``.

    :param min_size: A chunk is emitted only when the buffer holds more than this
        many items. Values below ``1`` are raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk. Values below ``min_size`` are
        raised to ``min_size``. Defaults to ``102400``.

    Input and output with ``min_size=3, max_size=4``::

        input:  "ab", "cdefghij"
        output: "abcd", "efgh", "ij"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseSizedChunksStage
        from pyrill.sources import IterableSource


        class Sized(BaseSizedChunksStage[str]):
            _buffer: str = ""


        source = IterableSource(source=["ab", "cdefghij"])
        stage = source >> Sized(min_size=3, max_size=4)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abcd", "efgh", "ij"]
    """

    def __init__(
        self,
        *args: Any,
        min_size: int = 1024,
        max_size: int = 1024 * 100,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._min_size: int = max(min_size, 1)
        self._max_size: int = max(self._min_size, max_size)

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the buffer holds more than ``min_size`` items.
        """
        return len(self._buffer) > self._min_size

    def _consume_buffer(self, closing: bool = False) -> AnyStr:
        result_len = min(len(self._buffer), self._max_size)

        result = self._buffer[:result_len]
        self._buffer = self._buffer[result_len:]

        return result


class BaseSizedChunksSource[AnyStr: (str, bytes)](
    BaseSizedChunksProducer[AnyStr], BaseDataChunkSource[AnyStr], ABC
):
    """
    Source flavour of :class:`BaseSizedChunksProducer`.

    Frames are fed with ``push_frame()`` and the buffer is flushed after
    ``end_of_stream()``. See :class:`BaseSizedChunksProducer` for the parameters and
    :class:`BaseDataChunkSource` for the push API.

    Input and output with ``min_size=3, max_size=4``::

        pushed:  "ab", "cdefghij", <end of stream>
        emitted: "abcd", "efgh", "ij"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseSizedChunksSource


        class Sized(BaseSizedChunksSource[str]):
            _buffer: str = ""


        source = Sized(min_size=3, max_size=4)
        await source.mount()

        await source.push_frame("ab")
        await source.push_frame("cdefghij")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["abcd", "efgh", "ij"]
    """

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the buffer is large enough or the input has been closed.
        """
        return not self._open_buffer or super().can_consume_buffer()


class BaseSizedChunksStage[AnyStr: (str, bytes)](
    BaseSizedChunksProducer[AnyStr], BaseDataChunkStage[AnyStr], ABC
):
    """
    Stage flavour of :class:`BaseSizedChunksProducer`.

    It re-chunks the frames of the upstream. See :class:`BaseSizedChunksProducer` for
    the parameters and the chunking rules.

    Input and output with ``min_size=3, max_size=4``::

        input:  "ab", "cdefghij"
        output: "abcd", "efgh", "ij"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseSizedChunksStage
        from pyrill.sources import IterableSource


        class Sized(BaseSizedChunksStage[str]):
            _buffer: str = ""


        source = IterableSource(source=["ab", "cdefghij"])
        stage = source >> Sized(min_size=3, max_size=4)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abcd", "efgh", "ij"]
    """


class BaseChunksSlowStartProducer[AnyStr: (str, bytes)](
    BaseSizedChunksProducer[AnyStr], ABC
):
    """
    Base class of elements that emit chunks whose emit threshold adapts to throughput.

    It extends :class:`BaseSizedChunksProducer`. The emit threshold starts at
    ``min_size`` and is recalculated on every buffer check: it is multiplied by
    ``multiplier`` (up to ``max_size``) when the previous check was less than
    ``interval`` seconds ago, and divided by it (down to ``min_size``) otherwise. The
    threshold is reset on unmount. See :class:`pyrill.bytes.BytesChunksSlowStart` for
    a detailed description of the behaviour.

    :param min_size: Initial and lowest emit threshold. Values below ``1`` are raised
        to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk and highest emit threshold.
        Defaults to ``102400``.
    :param interval: Maximum time, in seconds, between two buffer checks for the
        threshold to grow. Values below ``0.001`` are raised to ``0.001``. Defaults to
        ``1.0``.
    :param multiplier: Growth and shrink factor. Values below ``1.0`` are raised to
        ``1.0``. Defaults to ``2.0``.

    Input and output with ``min_size=4, max_size=64``::

        input:  "abcd", "efgh", "ijkl", "mnop"
        output: "abcdefghijklmnop"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSlowStartStage
        from pyrill.sources import IterableSource


        class SlowStart(BaseChunksSlowStartStage[str]):
            _buffer: str = ""


        source = IterableSource(source=["abcd", "efgh", "ijkl", "mnop"])
        stage = source >> SlowStart(min_size=4, max_size=64)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abcdefghijklmnop"]
    """

    def __init__(
        self, *args: Any, interval: float = 1.0, multiplier: float = 2.0, **kwargs: Any
    ):
        super().__init__(*args, **kwargs)

        self._interval = max(interval, 0.001)
        self._multiplier = max(multiplier, 1.0)

        self._initial_min_size = self._min_size
        self._last_dt: datetime | None = None

    async def _unmount(self) -> None:
        self._last_dt = None
        await super()._unmount()

    def _calculate_chunk_size(self) -> None:
        if self._last_dt is None:
            self._min_size = self._initial_min_size
        else:
            if (
                datetime.now(UTC) - self._last_dt
            ).total_seconds() < self._interval and self._min_size < self._max_size:
                self._min_size = round(self._min_size * self._multiplier)
            elif self._min_size > self._initial_min_size:
                self._min_size = round(self._min_size / self._multiplier)

            self._min_size = max(
                min(self._min_size, self._max_size), self._initial_min_size
            )

        self._last_dt = datetime.now(UTC)

    def can_consume_buffer(self) -> bool:
        """
        Recalculate the emit threshold and return ``True`` when the buffer exceeds it.
        """
        self._calculate_chunk_size()
        return super().can_consume_buffer()


class BaseChunksSlowStartSource[AnyStr: (str, bytes)](
    BaseChunksSlowStartProducer[AnyStr], BaseDataChunkSource[AnyStr], ABC
):
    """
    Source flavour of :class:`BaseChunksSlowStartProducer`.

    Frames are fed with ``push_frame()`` and the buffer is flushed after
    ``end_of_stream()``. See :class:`BaseChunksSlowStartProducer` for the parameters.

    Input and output with ``min_size=1, max_size=4``::

        pushed:  "abcdef", <end of stream>
        emitted: "abcd", "ef"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSlowStartSource


        class SlowStart(BaseChunksSlowStartSource[str]):
            _buffer: str = ""


        source = SlowStart(min_size=1, max_size=4)
        await source.mount()

        await source.push_frame("abcdef")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["abcd", "ef"]
    """

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the buffer exceeds the threshold or the input is closed.
        """
        return not self._open_buffer or super().can_consume_buffer()


class BaseChunksSlowStartStage[AnyStr: (str, bytes)](
    BaseChunksSlowStartProducer[AnyStr], BaseDataChunkStage[AnyStr], ABC
):
    """
    Stage flavour of :class:`BaseChunksSlowStartProducer`.

    It merges the frames of the upstream into chunks that grow with throughput. See
    :class:`BaseChunksSlowStartProducer` for the parameters.

    Input and output with ``min_size=4, max_size=64``::

        input:  "abcd", "efgh", "ijkl", "mnop"
        output: "abcdefghijklmnop"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSlowStartStage
        from pyrill.sources import IterableSource


        class SlowStart(BaseChunksSlowStartStage[str]):
            _buffer: str = ""


        source = IterableSource(source=["abcd", "efgh", "ijkl", "mnop"])
        stage = source >> SlowStart(min_size=4, max_size=64)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abcdefghijklmnop"]
    """


class BaseChunksSeparatorProducer[AnyStr: (str, bytes)](
    BaseDataChunkProducer[AnyStr], ABC
):
    """
    Base class of elements that emit chunks terminated by a separator.

    Each emitted chunk ends with (and includes) the separator, regardless of how the
    data is split into frames. When the stream ends, trailing data without a
    separator is flushed as a final chunk. Concrete classes are built with
    :class:`BaseChunksSeparatorSource` or :class:`BaseChunksSeparatorStage` plus a
    mixin that sets ``_buffer`` and implements the abstract class method
    ``default_separator()``, for example :class:`pyrill.strings.StringSeparatorMixin`.

    :param separator: Sequence that terminates each chunk. Defaults to the value of
        ``default_separator()``.

    Input and output with the separator ``";"``::

        input:  "a;b", ";c"
        output: "a;", "b;", "c"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSeparatorStage
        from pyrill.sources import IterableSource


        class Semicolons(BaseChunksSeparatorStage[str]):
            _buffer: str = ""

            @classmethod
            def default_separator(cls) -> str:
                return ";"


        source = IterableSource(source=["a;b", ";c"])
        stage = source >> Semicolons()

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["a;", "b;", "c"]
    """

    def __init__(self, *args: Any, separator: AnyStr | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.separator: AnyStr = (
            separator if separator is not None else self.default_separator()
        )

    @classmethod
    @abstractmethod
    def default_separator(cls) -> AnyStr:  # pragma: nocover
        """
        Return the separator used when none is given. Subclasses must implement it.
        """
        raise NotImplementedError()

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the buffer contains the separator.
        """
        return self.separator in self._buffer

    def _consume_buffer(self, closing: bool = False) -> AnyStr:
        if self.separator not in self._buffer:
            result, self._buffer = self._buffer, self._buffer[:0]
        else:
            result, self._buffer = self._buffer.split(self.separator, 1)
            result += self.separator
        return result


class BaseChunksSeparatorSource[AnyStr: (str, bytes)](
    BaseChunksSeparatorProducer[AnyStr], BaseDataChunkSource[AnyStr], ABC
):
    """
    Source flavour of :class:`BaseChunksSeparatorProducer`.

    Frames are fed with ``push_frame()`` and trailing data without a separator is
    flushed after ``end_of_stream()``. See :class:`BaseChunksSeparatorProducer` for
    the parameters.

    Input and output with the separator ``";"``::

        pushed:  "a;b", ";c", <end of stream>
        emitted: "a;", "b;", "c"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSeparatorSource


        class Semicolons(BaseChunksSeparatorSource[str]):
            _buffer: str = ""

            @classmethod
            def default_separator(cls) -> str:
                return ";"


        source = Semicolons()
        await source.mount()

        await source.push_frame("a;b")
        await source.push_frame(";c")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["a;", "b;", "c"]
    """

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the buffer contains the separator or the input is closed.
        """
        return not self._open_buffer or super().can_consume_buffer()


class BaseChunksSeparatorStage[AnyStr: (str, bytes)](
    BaseChunksSeparatorProducer[AnyStr], BaseDataChunkStage[AnyStr], ABC
):
    """
    Stage flavour of :class:`BaseChunksSeparatorProducer`.

    It splits the frames of the upstream into separator-terminated chunks. See
    :class:`BaseChunksSeparatorProducer` for the parameters.

    Input and output with the separator ``";"``::

        input:  "a;b", ";c"
        output: "a;", "b;", "c"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSeparatorStage
        from pyrill.sources import IterableSource


        class Semicolons(BaseChunksSeparatorStage[str]):
            _buffer: str = ""

            @classmethod
            def default_separator(cls) -> str:
                return ";"


        source = IterableSource(source=["a;b", ";c"])
        stage = source >> Semicolons()

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["a;", "b;", "c"]
    """


class BaseChunksFirstSeparatorProducer[AnyStr: (str, bytes)](
    BaseChunksSeparatorProducer[AnyStr], ABC
):
    """
    Base class of elements that split off only the first separator-terminated chunk.

    The first emitted chunk holds everything up to and including the first separator,
    which suits a leading header line. After that, buffered data is emitted as it
    arrives, without further splitting. The "first separator seen" flag is reset on
    mount. It has the same extension contract and ``separator`` parameter as
    :class:`BaseChunksSeparatorProducer`.

    :param separator: Sequence that terminates the first chunk. Defaults to the value
        of ``default_separator()``.

    Input and output with the separator ``";"``::

        input:  "head;bo", "dy;x"
        output: "head;", "bo", "dy;x"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksFirstSeparatorStage
        from pyrill.sources import IterableSource


        class Header(BaseChunksFirstSeparatorStage[str]):
            _buffer: str = ""

            @classmethod
            def default_separator(cls) -> str:
                return ";"


        source = IterableSource(source=["head;bo", "dy;x"])
        stage = source >> Header()

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["head;", "bo", "dy;x"]
    """

    _first_sep = False

    async def _mount(self) -> None:
        self._first_sep = False

        await super()._mount()

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when the header is ready or, after it, when the buffer is not empty.
        """
        return (not self._first_sep and self.separator in self._buffer) or (
            self._first_sep and len(self._buffer) > 0
        )

    def _consume_buffer(self, closing: bool = False) -> AnyStr:
        if not self._first_sep and self.separator in self._buffer:
            result, self._buffer = self._buffer.split(self.separator, 1)
            result += self.separator
            self._first_sep = True
        else:
            result, self._buffer = self._buffer, self._buffer[:0]
        return result


class BaseChunksFirstSeparatorSource[AnyStr: (str, bytes)](
    BaseChunksFirstSeparatorProducer[AnyStr], BaseDataChunkSource[AnyStr], ABC
):
    """
    Source flavour of :class:`BaseChunksFirstSeparatorProducer`.

    Frames are fed with ``push_frame()`` and the rest is flushed after
    ``end_of_stream()``. See :class:`BaseChunksFirstSeparatorProducer` for the
    parameters.

    Input and output with the separator ``";"``::

        pushed:  "head;bo", "dy;x", <end of stream>
        emitted: "head;", "body;x"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksFirstSeparatorSource


        class Header(BaseChunksFirstSeparatorSource[str]):
            _buffer: str = ""

            @classmethod
            def default_separator(cls) -> str:
                return ";"


        source = Header()
        await source.mount()

        await source.push_frame("head;bo")
        await source.push_frame("dy;x")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["head;", "body;x"]
    """

    def can_consume_buffer(self) -> bool:
        """
        Return ``True`` when a chunk can be emitted or the input has been closed.
        """
        return not self._open_buffer or super().can_consume_buffer()


class BaseChunksFirstSeparatorStage[AnyStr: (str, bytes)](
    BaseChunksFirstSeparatorProducer[AnyStr], BaseDataChunkStage[AnyStr], ABC
):
    """
    Stage flavour of :class:`BaseChunksFirstSeparatorProducer`.

    It splits off the first separator-terminated chunk of the upstream and passes the
    following frames through. See :class:`BaseChunksFirstSeparatorProducer` for the
    parameters.

    Input and output with the separator ``";"``::

        input:  "head;bo", "dy;x"
        output: "head;", "bo", "dy;x"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksFirstSeparatorStage
        from pyrill.sources import IterableSource


        class Header(BaseChunksFirstSeparatorStage[str]):
            _buffer: str = ""

            @classmethod
            def default_separator(cls) -> str:
                return ";"


        source = IterableSource(source=["head;bo", "dy;x"])
        stage = source >> Header()

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["head;", "bo", "dy;x"]
    """


class BaseDataAccumulatorStage[AnyStr: (str, bytes)](BaseDataChunkStage[AnyStr], ABC):
    """
    Base class of stages that join every frame of the stream into a single frame.

    Frames are buffered until the upstream ends, and the whole buffer is then emitted
    as one chunk. ``can_consume_buffer()`` always returns ``False``, so nothing is
    emitted before the end of the stream. Set ``max_buffer_size`` to bound memory use.
    Concrete classes add a mixin that sets ``_buffer``.

    :param max_buffer_size: Maximum number of items that the buffer may hold.
        Exceeding it raises :class:`OverflowError`. ``0`` disables the limit.
        Defaults to ``0``.

    Input and output::

        input:  "ab", "c", "de"
        output: "abcde"

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseDataAccumulatorStage
        from pyrill.sources import IterableSource


        class Accumulator(BaseDataAccumulatorStage[str]):
            _buffer: str = ""


        source = IterableSource(source=["ab", "c", "de"])
        stage = source >> Accumulator()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["abcde"]
    """

    def can_consume_buffer(self) -> bool:
        # Hold everything until upstream ends; consume_frame() then flushes it.
        """
        Return ``False`` always, so the buffer is only flushed at the end of the stream.
        """
        return False


class SkipUntil[AnyStr: (str, bytes)](BaseStage[AnyStr, AnyStr]):
    """
    Stage that discards the data before the first occurrence of a mark.

    Everything up to and including the first ``mark`` is dropped, and all the data
    after it is passed through unchanged. The mark may be split across several
    frames: the stage keeps the tail of the data seen so far to find it. Frames that
    are discarded completely are skipped (no frame is emitted for them), and a frame
    that contains the mark emits only the part after it, which can be empty. The
    discarded data is reported on the bus as a frame skipped message. The state is
    reset on mount and on unmount.

    :param mark: Sequence (``str`` or ``bytes``) that marks the start of the data to
        keep. Required, keyword only.

    Input and output with ``mark="##"``::

        input:  "noise #", "# data", " more"
        output: " data", " more"

    Usage:

    .. code-block:: python

        from pyrill.chunks import SkipUntil
        from pyrill.sources import IterableSource

        source = IterableSource(source=["noise #", "# data", " more"])
        stage = source >> SkipUntil(mark="##")

        await stage.mount()
        frames = [frame async for frame in stage]
        # [" data", " more"]
    """

    _skipped: bool = False

    def __init__(self, *args: Any, mark: AnyStr, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.mark: AnyStr = mark
        # Tail of the data seen so far, kept in case the mark spans two frames.
        self._pending: AnyStr = mark[:0]

    async def _mount(self) -> None:
        self._skipped = False
        self._pending = self.mark[:0]
        await super()._mount()

    async def _unmount(self) -> None:
        self._skipped = False
        self._pending = self.mark[:0]
        await super()._unmount()

    async def _process_frame(self, frame: AnyStr) -> AnyStr:
        if self._skipped:
            return frame

        frame = self._pending + frame
        if self.mark not in frame:
            cut = max(0, len(frame) - len(self.mark) + 1)
            self._pending = frame[cut:]
            raise FrameSkipped(frame[:cut])

        self._pending = frame[:0]
        skipped_part, data = frame.split(self.mark, 1)

        if skipped_part:
            self._logger.debug(
                f"Skipped frame: {skipped_part!r}", extra={"frame": skipped_part}
            )
            with suppress(Exception):
                self._send_message(
                    self._build_message(MSG_FRAME_SKIPPED, frame=skipped_part)
                )

        self._skipped = True
        return data

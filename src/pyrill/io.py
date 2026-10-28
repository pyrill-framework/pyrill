from abc import ABC
from asyncio import (
    BaseProtocol,
    Condition,
    Event,
    StreamReader,
    WriteTransport,
)
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any

from .base import BaseFullSink, BaseSource, ElementState

__all__ = [
    "MSG_FILE_READ_END",
    "MSG_FILE_READ_START",
    "ChunkedFileSource",
    "ChunkedIOSource",
    "ChunkedStreamReaderSource",
    "FileSink",
    "LineIOSource",
    "LineStreamReaderSource",
    "StreamReaderSink",
    "StreamReaderSource",
    "WriteTransportSource",
]

MSG_FILE_READ_START = "file-read-start"
MSG_FILE_READ_END = "file-read-end"
MSG_FILE_WRITE_START = "file-write-start"
MSG_FILE_WRITE_END = "file-write-end"


class BaseIOSource[AnyStr: (str, bytes)](BaseSource[AnyStr], ABC):
    """
    Base source that reads frames from an open file-like object.

    The base class stores the ``source`` object (text or binary, depending on
    ``AnyStr``). A subclass must implement ``_next_frame``, read from ``self._source``
    and raise :class:`StopAsyncIteration` when there is no more data. The source object
    is not opened or closed by the element: the caller owns it.
    :class:`ChunkedIOSource` and :class:`LineIOSource` are the ready-made versions.

    :param source: Open file-like object to read from. Required.

    Input and output::

        input:  "ab\\ncd\\n" (text of the file-like object)
        output: "AB\\n", "CD\\n"

    Usage:

    .. code-block:: python

        import io

        from pyrill.io import BaseIOSource


        class UpperLineSource(BaseIOSource[str]):
            async def _next_frame(self) -> str:
                line = self._source.readline()
                if not line:
                    raise StopAsyncIteration()
                return line.upper()


        source = UpperLineSource(source=io.StringIO("ab\\ncd\\n"))

        await source.mount()
        frames = [frame async for frame in source]
        # ["AB\\n", "CD\\n"]
    """

    def __init__(self, *args: Any, source: IO[AnyStr], **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._source: IO[AnyStr] = source


class ChunkedMixin:
    """
    Mixin that adds a ``chunk_size`` parameter to a source class.

    The size is stored in ``self._chunk_size``. The subclass reads it in its own
    ``_next_frame``, for example ``self._source.read(self._chunk_size)``. List the
    mixin before the source base class. :class:`ChunkedIOSource`,
    :class:`ChunkedStreamReaderSource` and :class:`ChunkedFileSource` use it.

    :param chunk_size: Maximum number of characters or bytes read at once. Defaults to
        ``1024``.

    Input and output with ``chunk_size=3``::

        input:  "abcdefg" (text of the file-like object)
        output: "abc", "def", "g"

    Usage:

    .. code-block:: python

        import io

        from pyrill.io import BaseIOSource, ChunkedMixin


        class MyChunks(ChunkedMixin, BaseIOSource[str]):
            async def _next_frame(self) -> str:
                data = self._source.read(self._chunk_size)
                if not data:
                    raise StopAsyncIteration()
                return data


        source = MyChunks(source=io.StringIO("abcdefg"), chunk_size=3)

        await source.mount()
        frames = [frame async for frame in source]
        # ["abc", "def", "g"]
    """

    def __init__(self, *args: Any, chunk_size: int = 1024, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._chunk_size = chunk_size


class ChunkedIOSource[AnyStr: (str, bytes)](
    ChunkedMixin,
    BaseIOSource[AnyStr],
):
    """
    Source that reads a file-like object in chunks of fixed size.

    Each frame holds up to ``chunk_size`` characters (text objects) or bytes (binary
    objects). The last chunk can be shorter, and the stream ends when the object is
    exhausted. The object is not closed by the source.

    :param source: Open file-like object to read from. Required.
    :param chunk_size: Maximum size of each chunk. Defaults to ``1024``.

    Input and output with ``chunk_size=3``::

        input:  "abcdefg" (text of the file-like object)
        output: "abc", "def", "g"

    Usage:

    .. code-block:: python

        import io

        from pyrill.io import ChunkedIOSource

        source = ChunkedIOSource(source=io.StringIO("abcdefg"), chunk_size=3)

        await source.mount()
        frames = [frame async for frame in source]
        # ["abc", "def", "g"]
    """

    async def _next_frame(self) -> AnyStr:
        data = self._source.read(self._chunk_size)
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class LineIOSource[AnyStr: (str, bytes)](
    BaseIOSource[AnyStr],
):
    """
    Source that reads a file-like object line by line.

    Each frame is one line, including its trailing newline when present (the last line
    may have none). Works with text and binary objects. The stream ends when the object
    is exhausted. The object is not closed by the source.

    :param source: Open file-like object to read from. Required.

    Input and output::

        input:  "a\\nb\\nc" (text of the file-like object)
        output: "a\\n", "b\\n", "c"

    Usage:

    .. code-block:: python

        import io

        from pyrill.io import LineIOSource

        source = LineIOSource(source=io.StringIO("a\\nb\\nc"))

        await source.mount()
        frames = [frame async for frame in source]
        # ["a\\n", "b\\n", "c"]
    """

    async def _next_frame(self) -> AnyStr:
        try:
            return self._source.__next__()
        except StopIteration:
            raise StopAsyncIteration()


class BaseStreamReaderSource(BaseSource[bytes], ABC):
    """
    Base source that reads byte frames from an :class:`asyncio.StreamReader`.

    The base class stores the ``stream``. A subclass must implement ``_next_frame``,
    read from ``self._stream`` and raise :class:`StopAsyncIteration` at end of file.
    :class:`StreamReaderSource`, :class:`ChunkedStreamReaderSource` and
    :class:`LineStreamReaderSource` are the ready-made versions.

    :param stream: Stream reader to read from. Required.

    Input and output::

        input:  b"abcde" (data fed to the stream reader)
        output: b"ab", b"cd", b"e"

    Usage:

    .. code-block:: python

        from asyncio import StreamReader

        from pyrill.io import BaseStreamReaderSource


        class TwoBytesSource(BaseStreamReaderSource):
            async def _next_frame(self) -> bytes:
                data = await self._stream.read(2)
                if not data:
                    raise StopAsyncIteration()
                return data


        stream = StreamReader()
        stream.feed_data(b"abcde")
        stream.feed_eof()
        source = TwoBytesSource(stream=stream)

        await source.mount()
        frames = [frame async for frame in source]
        # [b"ab", b"cd", b"e"]
    """

    def __init__(self, *args: Any, stream: StreamReader, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._stream: StreamReader = stream


class StreamReaderSource(BaseStreamReaderSource):
    """
    Source that emits the lines of an :class:`asyncio.StreamReader` as bytes frames.

    It iterates the stream reader, so each frame is one line including its newline
    (the last line may have none). The stream ends at end of file. A line longer than
    the reader limit raises an error.

    :param stream: Stream reader to read from. Required.

    Input and output::

        input:  b"a\\nb\\nc" (data fed to the stream reader)
        output: b"a\\n", b"b\\n", b"c"

    Usage:

    .. code-block:: python

        from asyncio import StreamReader

        from pyrill.io import StreamReaderSource

        stream = StreamReader()
        stream.feed_data(b"a\\nb\\nc")
        stream.feed_eof()
        source = StreamReaderSource(stream=stream)

        await source.mount()
        frames = [frame async for frame in source]
        # [b"a\\n", b"b\\n", b"c"]
    """

    async def _next_frame(self) -> bytes:
        return await self._stream.__anext__()


class ChunkedStreamReaderSource(ChunkedMixin, BaseStreamReaderSource):
    """
    Source that reads an :class:`asyncio.StreamReader` in chunks of up to ``chunk_size``.

    Each frame holds the bytes that are available, up to ``chunk_size``, so a chunk can
    be shorter when less data has arrived. The stream ends at end of file.

    :param stream: Stream reader to read from. Required.
    :param chunk_size: Maximum size of each chunk in bytes. Defaults to ``1024``.

    Input and output with ``chunk_size=3``::

        input:  b"abcdefg" (data fed to the stream reader)
        output: b"abc", b"def", b"g"

    Usage:

    .. code-block:: python

        from asyncio import StreamReader

        from pyrill.io import ChunkedStreamReaderSource

        stream = StreamReader()
        stream.feed_data(b"abcdefg")
        stream.feed_eof()
        source = ChunkedStreamReaderSource(stream=stream, chunk_size=3)

        await source.mount()
        frames = [frame async for frame in source]
        # [b"abc", b"def", b"g"]
    """

    async def _next_frame(self) -> bytes:
        data = await self._stream.read(self._chunk_size)
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class LineStreamReaderSource(BaseStreamReaderSource):
    """
    Source that reads an :class:`asyncio.StreamReader` line by line.

    Each frame is one line as bytes, including its newline (the last line may have
    none). The stream ends at end of file. A line longer than the reader limit raises
    :class:`ValueError`.

    :param stream: Stream reader to read from. Required.

    Input and output::

        input:  b"a\\nb\\nc" (data fed to the stream reader)
        output: b"a\\n", b"b\\n", b"c"

    Usage:

    .. code-block:: python

        from asyncio import StreamReader

        from pyrill.io import LineStreamReaderSource

        stream = StreamReader()
        stream.feed_data(b"a\\nb\\nc")
        stream.feed_eof()
        source = LineStreamReaderSource(stream=stream)

        await source.mount()
        frames = [frame async for frame in source]
        # [b"a\\n", b"b\\n", b"c"]
    """

    async def _next_frame(self) -> bytes:
        data = await self._stream.readline()
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class StreamReaderSink(BaseFullSink[bytes]):
    """
    Sink that feeds every bytes frame into an :class:`asyncio.StreamReader`.

    Each frame is passed to ``stream.feed_data``. When the sink is unmounted (at the
    end of the stream), it calls ``stream.feed_eof`` so readers of the stream see the
    end of file. Use it to expose a pipeline as a stream reader.

    :param stream: Stream reader that receives the data. Required.

    Input and output::

        input:  b"he", b"llo"
        output: (none) the stream reader holds b"hello" and is at end of file

    Usage:

    .. code-block:: python

        from asyncio import StreamReader

        from pyrill.io import StreamReaderSink
        from pyrill.sources import IterableSource

        stream = StreamReader()
        source = IterableSource(source=[b"he", b"llo"])
        sink = source >> StreamReaderSink(stream=stream)

        await sink.consume_all()
        data = await stream.read(-1)
        # b"hello"
    """

    def __init__(self, *args: Any, stream: StreamReader, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._stream: StreamReader = stream

    async def _process_frame(self, frame: bytes) -> None:
        return self._stream.feed_data(frame)

    async def _unmount(self) -> None:
        self._stream.feed_eof()
        await super()._unmount()


class WriteTransportSource(BaseSource[bytes], WriteTransport):
    """
    Source that works as an :class:`asyncio.WriteTransport` and emits what is written.

    Use it to plug code that expects a transport (for example an
    :class:`asyncio.StreamWriter`) into a pipeline. Data written with :meth:`write`,
    :meth:`writelines` or a stream writer is buffered, and each read of the source
    returns the whole buffer as one bytes frame. The stream ends after
    :meth:`write_eof` once the buffer has been consumed, or when the source is closed.

    A protocol must be set with :meth:`set_protocol` before mounting: mounting calls
    ``connection_made`` and unmounting calls ``connection_lost``. When the buffer
    reaches the high watermark, ``pause_writing`` is called on the protocol, and
    ``resume_writing`` when a frame has been read and the buffer is below the low
    watermark. The watermarks are ``(low, high) = (1, 1024)`` by default and can be
    changed with :meth:`set_write_buffer_limits`. :meth:`drain` waits until the
    buffered data has been read by the consumer, so it needs a consumer running
    concurrently.

    Input and output::

        input:  transport.write(b"hello "), transport.write(b"world"), write_eof()
        output: b"hello ", b"world"
                (data written before the consumer reads is delivered as one frame)

    Usage:

    .. code-block:: python

        import asyncio
        from asyncio import Protocol

        from pyrill.io import WriteTransportSource

        transport = WriteTransportSource()
        transport.set_protocol(Protocol())
        await transport.mount()


        async def collect() -> list[bytes]:
            return [frame async for frame in transport]


        task = asyncio.create_task(collect())
        transport.write(b"hello ")
        await transport.drain()
        transport.write(b"world")
        transport.write_eof()
        await transport.drain()

        frames = await task
        # [b"hello ", b"world"]
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        # BaseTransport.__init__ is never reached via super() because
        # BaseElement.__init__ does not call super(). Initialise _extra
        # here so get_extra_info() works when a StreamWriter wraps this.
        self._extra: dict[str, Any] = {}

        self._protocol: BaseProtocol | None = None
        self._buffer: bytes = b""
        self._buffer_limits: tuple[int, int] = (1, 1024)
        self._eos: bool = False
        self._paused: bool = False
        self._condition = Condition()
        self._drained_event = Event()

    def set_protocol(self, protocol: BaseProtocol) -> None:
        """
        Set the protocol that is notified of connection, flow control and close events.

        It must be called before mounting.
        """

        self._protocol = protocol

    def get_protocol(self) -> BaseProtocol:
        """
        Return the protocol, or raise :class:`RuntimeError` when none has been set.
        """

        if self._protocol is None:
            raise RuntimeError("No protocol connected to this transport")
        return self._protocol

    def is_closing(self) -> bool:
        """Return True if the transport is closing or closed."""
        return self._closed or self.state is not ElementState.READY or self._eos

    def close(self) -> None:
        """
        Close the transport: schedule the unmount of the source, which ends the stream.
        """

        self.pipeline.create_orphan_task(self.unmount())

    def set_write_buffer_limits(
        self, high: int | None = None, low: int | None = None
    ) -> None:
        """
        Set the flow-control watermarks of the write buffer, in bytes.

        ``high`` pauses the protocol when the buffer reaches it, and ``low`` is the level
        under which the protocol is resumed. An omitted value keeps its current setting.
        """

        self._buffer_limits = (
            low if low is not None else self._buffer_limits[0],
            high if high is not None else self._buffer_limits[1],
        )

    def get_write_buffer_size(self) -> int:
        """Return the current size of the write buffer."""
        return len(self._buffer)

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Get the high and low watermarks for write flow control.
        Return a tuple (low, high) where low and high are
        positive number of bytes."""
        return self._buffer_limits

    def write(self, data: bytes | bytearray | memoryview[Any]) -> None:
        """Write some data bytes to the transport.

        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        """

        self._buffer += data
        if not self._paused and len(self._buffer) >= self._buffer_limits[1]:
            self._paused = True
            self.get_protocol().pause_writing()
        if len(self._buffer) >= self._buffer_limits[0]:
            self.pipeline.create_orphan_task(self.drain())

    def writelines(
        self, list_of_data: Iterable[bytes | bytearray | memoryview[Any]]
    ) -> None:
        """Write a list (or any iterable) of data bytes to the transport.

        The default implementation concatenates the arguments and
        calls write() on the result.
        """
        data = b"".join(list_of_data)
        self.write(data)

    def write_eof(self) -> None:
        """Close the write end after flushing buffered data.

        (This is like typing ^D into a UNIX program reading from stdin.)

        Data may still be received.
        """
        self._eos = True
        self.pipeline.create_orphan_task(self.drain())

    def can_write_eof(self) -> bool:
        """Return True if this transport supports write_eof(), False if not."""
        return not self._eos and self.state is ElementState.READY

    def abort(self) -> None:
        """Close the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        self.pipeline.create_orphan_task(
            self._set_error(RuntimeError("Transport aborted"))
        )

    async def drain(self) -> None:
        """
        Wait until the consumer has read the buffered data.

        Raise :class:`RuntimeError` when the transport is closed. A consumer must be
        iterating the source concurrently, otherwise this call waits forever.
        """

        if self._closed:
            raise RuntimeError("Transport is closed")
        if len(self._buffer):
            async with self._condition:
                self._condition.notify()
        await self._drained_event.wait()
        self._drained_event.clear()

    async def _next_frame(self) -> bytes:
        async with self._condition:
            await self._condition.wait_for(
                lambda: self._closed or len(self._buffer) > 0 or self._eos
            )
            try:
                if self._closed or len(self._buffer) == 0:
                    raise StopAsyncIteration()

                data = self._buffer
                self._buffer = b""
                return data
            finally:
                self._drained_event.set()
                if self._paused and len(self._buffer) <= self._buffer_limits[0]:
                    self._paused = False
                    self.get_protocol().resume_writing()

    async def _mount(self) -> None:
        await super()._mount()
        self._eos = False
        self._paused = False
        self._buffer = b""
        async with self._condition:
            self._condition.notify_all()
        self._drained_event.clear()
        self.get_protocol().connection_made(self)

    async def _unmount(self) -> None:
        self._eos = True
        await super()._unmount()
        async with self._condition:
            self._condition.notify_all()
        self.get_protocol().connection_lost(
            self._last_error if isinstance(self._last_error, Exception) else None
        )


class BaseFileSource[AnyStr: (str, bytes)](BaseSource[AnyStr], ABC):
    """
    Base source that reads frames from a file that it opens itself.

    The file at ``path`` is opened lazily when the subclass calls
    ``self._prepare_stream()``, usually from ``_next_frame``, and it is closed when the
    element is unmounted. A subclass must implement ``_next_frame``, read from
    ``self._stream`` after preparing it, and raise :class:`StopAsyncIteration` at end
    of file. The element sends the messages ``file-read-start`` (with ``path`` and
    ``size``) when the file is opened and ``file-read-end`` (with ``path``) when it is
    closed. :class:`ChunkedFileSource` is the ready-made version.

    :param path: Path of the file to read. Required.
    :param mode: Mode used to open the file: ``"r"`` for text (default) or ``"rb"``
        for bytes.

    Input and output::

        input:  file with the text "ab\\ncd\\n"
        output: "AB\\n", "CD\\n"

    Usage:

    .. code-block:: python

        import tempfile
        from pathlib import Path

        from pyrill.io import BaseFileSource


        class UpperLineFileSource(BaseFileSource[str]):
            async def _next_frame(self) -> str:
                self._prepare_stream()
                assert self._stream is not None
                line = self._stream.readline()
                if not line:
                    raise StopAsyncIteration()
                return line.upper()


        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.txt"
            path.write_text("ab\\ncd\\n")
            source = UpperLineFileSource(path=path)

            await source.mount()
            collected = [frame async for frame in source]

        frames = collected
        # ["AB\\n", "CD\\n"]
    """

    def __init__(self, *args: Any, path: Path, mode: str = "r", **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._path = path
        self._mode = mode
        self._stream: IO[AnyStr] | None = None
        self._start_read = True

    def _prepare_stream(self) -> None:
        if self._stream is not None:
            return

        self._stream = self._path.open(mode=self._mode)
        self._send_message(
            self._build_message(
                MSG_FILE_READ_START, path=self._path, size=self._path.stat().st_size
            )
        )

    def _unprepare_stream(self) -> None:
        if self._stream is None:
            return
        self._stream.close()
        self._stream = None
        self._send_message(self._build_message(MSG_FILE_READ_END, path=self._path))

    async def _unmount(self) -> None:
        self._unprepare_stream()
        await super()._unmount()


class ChunkedFileSource[AnyStr: (str, bytes)](ChunkedMixin, BaseFileSource[AnyStr]):
    """
    Source that reads a file in chunks of fixed size.

    The file is opened on the first read and closed when the stream ends. Each frame
    holds up to ``chunk_size`` characters (text mode) or bytes (binary mode). The last
    chunk can be shorter. The messages of :class:`BaseFileSource` are sent when the file
    is opened and closed.

    :param path: Path of the file to read. Required.
    :param mode: ``"r"`` for text frames (default) or ``"rb"`` for bytes frames.
    :param chunk_size: Maximum size of each chunk. Defaults to ``1024``.

    Input and output with ``chunk_size=4``::

        input:  file with the text "abcdefghij"
        output: "abcd", "efgh", "ij"

    Usage:

    .. code-block:: python

        import tempfile
        from pathlib import Path

        from pyrill.io import ChunkedFileSource

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.txt"
            path.write_text("abcdefghij")
            source = ChunkedFileSource(path=path, chunk_size=4)

            await source.mount()
            collected = [frame async for frame in source]

        frames = collected
        # ["abcd", "efgh", "ij"]
    """

    async def _next_frame(self) -> AnyStr:
        self._prepare_stream()
        assert self._stream is not None

        data = self._stream.read(self._chunk_size)
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class FileSink[AnyStr: (str, bytes)](BaseFullSink[AnyStr]):
    """
    Sink that writes every frame to a file.

    The file is opened on the first frame and each frame is written and flushed
    immediately. It is closed when the sink is unmounted, at the end of the stream. No
    file is created when no frame arrives. The sink sends the messages
    ``file-write-start`` (with ``path``) when the file is opened and ``file-write-end``
    (with ``path`` and ``size``) when it is closed. An existing file is overwritten with
    the default mode.

    :param path: Path of the file to write. Required.
    :param mode: Mode used to open the file: ``"w"`` for text frames (default),
        ``"wb"`` for bytes frames, or ``"a"`` to append.
    :param open_kwargs: Extra keyword arguments for :meth:`pathlib.Path.open`, for
        example ``{"encoding": "utf-8"}``. Defaults to none.

    Input and output::

        input:  "hello\\n", "world\\n"
        output: (none) the file holds "hello\\nworld\\n"

    Usage:

    .. code-block:: python

        import tempfile
        from pathlib import Path

        from pyrill.io import FileSink
        from pyrill.sources import IterableSource

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "output.txt"
            source = IterableSource(source=["hello\\n", "world\\n"])
            sink = source >> FileSink(path=path)

            await sink.consume_all()
            written = path.read_text()

        text = written
        # "hello\\nworld\\n"
    """

    def __init__(
        self,
        *args: Any,
        path: Path,
        mode: str = "w",
        open_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._path = path
        self._mode = mode
        self._open_kwargs = open_kwargs or {}
        self._sink: IO[AnyStr] | None = None

    def _prepare_stream(self) -> None:
        if self._sink is not None:
            return

        self._sink = self._path.open(mode=self._mode, **self._open_kwargs)
        self._send_message(self._build_message(MSG_FILE_WRITE_START, path=self._path))

    def _unprepare_stream(self) -> None:
        if self._sink is None:
            return
        file_length = self._sink.tell()
        self._sink.close()
        self._sink = None
        self._send_message(
            self._build_message(MSG_FILE_WRITE_END, path=self._path, size=file_length)
        )

    async def _process_frame(self, frame: AnyStr) -> None:
        self._prepare_stream()

        assert self._sink is not None

        self._sink.write(frame)
        self._sink.flush()

    async def _unmount(self) -> None:
        self._unprepare_stream()
        await super()._unmount()

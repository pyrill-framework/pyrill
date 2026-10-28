from codecs import getincrementaldecoder
from typing import Any

from pyrill.base import FrameSkipped

from .chunks import (
    BaseChunksFirstSeparatorSource,
    BaseChunksFirstSeparatorStage,
    BaseChunksSeparatorSource,
    BaseChunksSeparatorStage,
    BaseChunksSlowStartSource,
    BaseChunksSlowStartStage,
    BaseDataAccumulatorStage,
    BaseDataChunkStage,
    BaseSizedChunksSource,
    BaseSizedChunksStage,
)
from .mappers import make_map

__all__ = [
    "BytesChunksFirstSeparator",
    "BytesChunksFirstSeparatorSource",
    "BytesChunksMixin",
    "BytesChunksSeparator",
    "BytesChunksSeparatorSource",
    "BytesChunksSlowStart",
    "BytesChunksSlowStartSource",
    "BytesDataAccumulator",
    "BytesSeparatorMixin",
    "BytesSizedChunks",
    "BytesSizedChunksSource",
    "Decode",
    "UnicodeChunks",
]


class BytesChunksMixin:
    """
    Mixin that sets the empty ``b""`` buffer of the bytes chunk classes.

    The generic classes in :mod:`pyrill.chunks` work on ``str`` or ``bytes`` and need
    the empty buffer value of the type. This mixin supplies it as ``_buffer`` and must
    come before the generic base class in the bases. Use it to build a custom chunker
    for bytes data.

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseDataAccumulatorStage
        from pyrill.bytes import BytesChunksMixin
        from pyrill.sources import IterableSource


        class Joiner(BytesChunksMixin, BaseDataAccumulatorStage[bytes]):
            pass


        source = IterableSource(source=[b"ab", b"cd"])
        stage = source >> Joiner()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [b"abcd"]
    """

    _buffer: bytes = b""


class BytesSeparatorMixin(BytesChunksMixin):
    r"""
    Mixin that adds the default separator ``\n`` to :class:`BytesChunksMixin`.

    It sets the empty buffer value and implements the abstract class method
    ``default_separator()`` required by the separator classes of
    :mod:`pyrill.chunks`. Override ``default_separator()`` in a subclass to change
    the default.

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSeparatorStage
        from pyrill.bytes import BytesSeparatorMixin
        from pyrill.sources import IterableSource


        class Lines(BytesSeparatorMixin, BaseChunksSeparatorStage[bytes]):
            pass


        source = IterableSource(source=[b"a\nb"])
        stage = source >> Lines()

        await stage.mount()
        lines = [line async for line in stage]
        # [b"a\n", b"b"]
    """

    @classmethod
    def default_separator(cls) -> bytes:
        r"""
        Return the default separator, ``\n``.
        """
        return b"\n"


# Sources


class BytesSizedChunksSource(BytesChunksMixin, BaseSizedChunksSource[bytes]):
    """
    Source that re-chunks pushed bytes into chunks of bounded size.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    Pushed bytes are accumulated in an internal buffer. A chunk is emitted as soon
    as the buffer holds more than ``min_size`` bytes, and each chunk holds at most
    ``max_size`` bytes. When the stream ends, any remaining bytes are flushed as a
    final, possibly smaller, chunk.

    :param min_size: A chunk is emitted only when the buffer holds more than this
        many bytes. Values below ``1`` are raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk. Values below ``min_size``
        are raised to ``min_size``. Defaults to ``102400``.

    Input and output with ``min_size=3, max_size=4``::

        pushed:  b"ab", b"cdefghij", <end of stream>
        emitted: b"abcd", b"efgh", b"ij"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesSizedChunksSource

        source = BytesSizedChunksSource(min_size=3, max_size=4)
        await source.mount()

        await source.push_frame(b"ab")
        await source.push_frame(b"cdefghij")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # [b"abcd", b"efgh", b"ij"]
    """


class BytesChunksSlowStartSource(BytesChunksMixin, BaseChunksSlowStartSource[bytes]):
    """
    Source that re-chunks pushed bytes using an adaptive (slow-start) chunk size.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    The minimum chunk size adapts to throughput. Each time the element checks
    whether a chunk can be emitted, it measures the time since the previous check:

    * faster than ``interval`` seconds: ``min_size`` is multiplied by
      ``multiplier``, up to ``max_size``;
    * otherwise: ``min_size`` is divided by ``multiplier``, down to the initial
      ``min_size``.

    Slow streams therefore produce small, low-latency chunks, while fast streams
    progressively produce larger chunks. When the stream ends, any remaining bytes
    are flushed as a final chunk.

    :param min_size: Initial (and lowest) minimum chunk size. A chunk is emitted
        only when the buffer holds more than the current minimum. Values below
        ``1`` are raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk and upper bound of the
        adaptive minimum. Defaults to ``102400``.
    :param interval: Time window, in seconds, used to decide whether the stream is
        busy. Values below ``0.001`` are raised to ``0.001``. Defaults to ``1.0``.
    :param multiplier: Growth and shrink factor applied to the adaptive minimum.
        Values below ``1.0`` are raised to ``1.0``. Defaults to ``2.0``.

    Input and output with ``min_size=1, max_size=4``::

        pushed:  b"abcdef", <end of stream>
        emitted: b"abcd", b"ef"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesChunksSlowStartSource

        source = BytesChunksSlowStartSource(min_size=1, max_size=4)
        await source.mount()

        await source.push_frame(b"abcdef")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # [b"abcd", b"ef"]
    """


class BytesChunksSeparatorSource(BytesSeparatorMixin, BaseChunksSeparatorSource[bytes]):
    r"""
    Source that splits pushed bytes into chunks terminated by a separator.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    Each emitted chunk ends with (and includes) the separator. A chunk may span
    several pushed frames. When the stream ends, trailing bytes without a
    separator are flushed as a final chunk.

    :param separator: Byte sequence that terminates each chunk. Defaults to
        ``b"\n"``.

    Input and output with the default separator::

        pushed:  b"line 1\nline", b" 2\npartial", <end of stream>
        emitted: b"line 1\n", b"line 2\n", b"partial"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesChunksSeparatorSource

        source = BytesChunksSeparatorSource()
        await source.mount()

        await source.push_frame(b"line 1\nline")
        await source.push_frame(b" 2\npartial")
        await source.end_of_stream()

        lines = [line async for line in source]
        # [b"line 1\n", b"line 2\n", b"partial"]
    """


class BytesChunksFirstSeparatorSource(
    BytesSeparatorMixin, BaseChunksFirstSeparatorSource[bytes]
):
    r"""
    Source that splits off the first separator-terminated chunk of pushed bytes.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    The first emitted chunk holds everything up to and including the first
    separator, which suits a leading header line. After that, buffered data is
    emitted as-is, without further splitting.

    :param separator: Byte sequence that terminates the first chunk. Defaults to
        ``b"\n"``.

    Input and output with the default separator::

        pushed:  b"header\nbody", b" more\ndata", <end of stream>
        emitted: b"header\n", b"body more\ndata"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesChunksFirstSeparatorSource

        source = BytesChunksFirstSeparatorSource()
        await source.mount()

        await source.push_frame(b"header\nbody")
        await source.push_frame(b" more\ndata")
        await source.end_of_stream()

        header, body = [chunk async for chunk in source]
        # header == b"header\n"
        # body == b"body more\ndata"
    """


# Middle stages


class BytesSizedChunks(BytesChunksMixin, BaseSizedChunksStage[bytes]):
    """
    Stage that re-chunks a byte stream into chunks of bounded size.

    Incoming frames are accumulated in an internal buffer. A chunk is emitted as
    soon as the buffer holds more than ``min_size`` bytes, and each chunk holds at
    most ``max_size`` bytes. When the upstream ends, any remaining bytes are
    flushed as a final, possibly smaller, chunk.

    :param min_size: A chunk is emitted only when the buffer holds more than this
        many bytes. Values below ``1`` are raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk. Values below ``min_size``
        are raised to ``min_size``. Defaults to ``102400``.

    Input and output with ``min_size=3, max_size=4``::

        input:  b"ab", b"cdefghij"
        output: b"abcd", b"efgh", b"ij"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesSizedChunks
        from pyrill.sources import IterableSource

        source = IterableSource(source=[b"ab", b"cdefghij"])
        stage = source >> BytesSizedChunks(min_size=3, max_size=4)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # [b"abcd", b"efgh", b"ij"]
    """


class BytesChunksSlowStart(BytesChunksMixin, BaseChunksSlowStartStage[bytes]):
    """
    Stage that merges a byte stream into chunks that grow with throughput.

    Incoming frames are buffered. When the buffer holds more than the current
    *emit threshold*, the whole buffer is emitted as one chunk, capped at
    ``max_size`` bytes. The threshold controls when a chunk is emitted, not its
    size: a chunk is whatever has accumulated by then.

    The threshold starts at ``min_size`` and is recalculated on every buffer
    check. The stage checks once before each upstream read and once after each
    frame is appended, so every arriving frame triggers a recalculation:

    * the previous check was less than ``interval`` seconds ago and the
      threshold is below ``max_size``: the threshold is multiplied by
      ``multiplier``;
    * otherwise, if the threshold is above ``min_size``: it is divided by
      ``multiplier``.

    The new threshold is rounded to the nearest integer and kept between
    ``min_size`` and ``max_size``.

    On a fast stream, the threshold therefore grows geometrically with each
    frame (``4, 8, 16, 32…`` for ``min_size=4, multiplier=2``). Each emit then
    waits for more data, and frames are merged into progressively larger chunks.
    When frames arrive more than ``interval`` seconds apart, the threshold drops
    back one division per check. A slow stream thus returns to small,
    low-latency chunks.

    Rounding can cancel growth when both the threshold and ``multiplier`` are
    small. For example, ``min_size=1, multiplier=1.4`` never grows, because
    ``round(1.4) == 1``. The threshold is reset to ``min_size`` on unmount. When
    the stream ends, any remaining bytes are flushed as a final chunk.

    :param min_size: Initial and lowest emit threshold. Values below ``1`` are
        raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk and highest emit
        threshold. Values below ``min_size`` are raised to ``min_size``.
        Defaults to ``102400``.
    :param interval: Maximum time, in seconds, between two consecutive buffer
        checks for the threshold to grow. Values below ``0.001`` are raised to
        ``0.001``. Defaults to ``1.0``.
    :param multiplier: Factor applied to the threshold on each growth or shrink
        step. Values below ``1.0`` are raised to ``1.0``, which disables
        adaptation. Defaults to ``2.0``.

    Threshold evolution for four 4-byte frames arriving at once, with
    ``min_size=4, max_size=64``::

        check:      1   2   3   4   5
        buffered:   0   4   8  12  16
        threshold:  4   8  16  32  64

    The buffer never exceeds the threshold, so the 16 bytes are flushed as a
    single chunk at end of stream::

        input:  b"abcd", b"efgh", b"ijkl", b"mnop"
        output: b"abcdefghijklmnop"

    For comparison, :class:`BytesSizedChunks` with ``min_size=4`` keeps a fixed
    threshold and emits ``b"abcdefgh", b"ijklmnop"``.

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesChunksSlowStart
        from pyrill.sources import IterableSource

        source = IterableSource(source=[b"abcd", b"efgh", b"ijkl", b"mnop"])
        stage = source >> BytesChunksSlowStart(min_size=4, max_size=64)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # [b"abcdefghijklmnop"]
    """


class BytesChunksSeparator(BytesSeparatorMixin, BaseChunksSeparatorStage[bytes]):
    r"""
    Stage that splits a byte stream into chunks terminated by a separator.

    Each emitted chunk ends with (and includes) the separator, regardless of how
    the input frames are split. When the upstream ends, trailing bytes without a
    separator are flushed as a final chunk.

    :param separator: Byte sequence that terminates each chunk. Defaults to
        ``b"\n"``.

    Input and output with ``separator=b"\r\n"``::

        input:  b"a\r\nb\r", b"\nc"
        output: b"a\r\n", b"b\r\n", b"c"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesChunksSeparator
        from pyrill.sources import IterableSource

        source = IterableSource(
            source=[b"GET / HTTP/1.1\nHost: x", b"\nAccept: */*\n"]
        )
        stage = source >> BytesChunksSeparator()

        await stage.mount()
        lines = [line async for line in stage]
        # [b"GET / HTTP/1.1\n", b"Host: x\n", b"Accept: */*\n"]
    """


class BytesChunksFirstSeparator(
    BytesSeparatorMixin, BaseChunksFirstSeparatorStage[bytes]
):
    r"""
    Stage that splits off the first separator-terminated chunk of a byte stream.

    The first emitted chunk holds everything up to and including the first
    separator, which suits a leading header or status line. After that, incoming
    frames are passed through unchanged, without further splitting.

    :param separator: Byte sequence that terminates the first chunk. Defaults to
        ``b"\n"``.

    Input and output with ``separator=b"\r\n"``::

        input:  b"HTTP/1.1 200 OK\r\nbody ", b"part 1", b" part 2"
        output: b"HTTP/1.1 200 OK\r\n", b"body ", b"part 1", b" part 2"

    Usage:

    .. code-block:: python

        from pyrill.bytes import BytesChunksFirstSeparator
        from pyrill.sources import IterableSource

        source = IterableSource(
            source=[b"HTTP/1.1 200 OK\r\nbody ", b"part 1", b" part 2"]
        )
        stage = source >> BytesChunksFirstSeparator(separator=b"\r\n")

        await stage.mount()
        status_line = await stage.__anext__()
        # b"HTTP/1.1 200 OK\r\n"
        body = [chunk async for chunk in stage]
        # [b"body ", b"part 1", b" part 2"]
    """


@make_map
def Decode(frame: bytes, /, *, encoding: str = "utf-8", **kwargs: Any) -> str:
    r"""
    Map stage that decodes each bytes frame into a ``str``.

    Every frame is decoded independently, so a frame that ends in the middle of a
    multi-byte character raises :class:`UnicodeDecodeError`. Place
    :class:`UnicodeChunks` before this stage when the upstream may split
    characters.

    :param encoding: Text encoding used to decode each frame. Defaults to
        ``"utf-8"``.

    Input and output with the default encoding::

        input:  b"caf\xc3\xa9", b"na\xc3\xafve"
        output: "café", "naïve"

    Usage:

    .. code-block:: python

        from pyrill.bytes import Decode
        from pyrill.sources import IterableSource

        source = IterableSource(source=[b"caf\xe9"])
        stage = source >> Decode(encoding="latin-1")

        await stage.mount()
        texts = [text async for text in stage]
        # ["café"]
    """
    return frame.decode(encoding=encoding)


def _get_valid_unicode_bytes(data: bytes, encoding: str = "utf-8") -> bytes:
    # The incremental decoder holds back an incomplete trailing character, so
    # everything before those pending bytes is the longest decodable prefix.
    # Invalid bytes raise UnicodeDecodeError once no valid prefix precedes them.
    decoder = getincrementaldecoder(encoding)()
    try:
        decoder.decode(data)
    except UnicodeDecodeError as e:
        if e.start == 0:
            raise
        return data[: e.start]
    pending, _ = decoder.getstate()
    return data[: len(data) - len(pending)]


def _has_unicode_decodable_bytes(data: bytes, encoding: str = "utf-8") -> bool:
    return len(_get_valid_unicode_bytes(data[:16], encoding=encoding)) > 0


class UnicodeChunks(BytesChunksMixin, BaseDataChunkStage[bytes]):
    r"""
    Stage that re-chunks a byte stream on character boundaries.

    Each emitted chunk is decodable on its own with ``encoding``. When a
    multi-byte character is split across input frames, its leading bytes are held
    back and emitted together with the rest of the character. Place it before
    :class:`Decode` when the upstream may split characters.

    :param encoding: Text encoding used to find character boundaries. Defaults to
        ``"utf-8"``.

    Input and output with the default encoding (``"é"`` is ``b"\xc3\xa9"``)::

        input:  b"caf\xc3", b"\xa9 ok"
        output: b"caf", b"\xc3\xa9 ok"

    Usage:

    .. code-block:: python

        from pyrill.bytes import Decode, UnicodeChunks
        from pyrill.sources import IterableSource

        source = IterableSource(source=[b"caf\xc3", b"\xa9 ok"])
        stage = source >> UnicodeChunks() >> Decode()

        await stage.mount()
        texts = [text async for text in stage]
        # ["caf", "é ok"]
    """

    def __init__(
        self,
        *args: Any,
        encoding: str = "utf-8",
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.encoding = encoding

    def can_consume_buffer(self) -> bool:
        return _has_unicode_decodable_bytes(self._buffer, encoding=self.encoding)

    def _consume_buffer(self, closing: bool = False) -> bytes:
        result = _get_valid_unicode_bytes(self._buffer, encoding=self.encoding)
        if closing and len(result) == 0:
            skipped = self._buffer
            self._buffer = self._buffer[:0]
            raise FrameSkipped(skipped)

        self._buffer = self._buffer[len(result) :]

        return result


class BytesDataAccumulator(BytesChunksMixin, BaseDataAccumulatorStage[bytes]):
    """
    Stage that joins every frame of a byte stream into a single frame.

    Frames are buffered until the upstream ends, then emitted as one chunk.
    Set ``max_buffer_size`` to bound memory use.

    Input and output::

        input:  b"ab", b"c", b"de"
        output: b"abcde"
    """

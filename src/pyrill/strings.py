from typing import Any

from .chunks import (
    BaseChunksFirstSeparatorSource,
    BaseChunksFirstSeparatorStage,
    BaseChunksSeparatorSource,
    BaseChunksSeparatorStage,
    BaseChunksSlowStartSource,
    BaseChunksSlowStartStage,
    BaseDataAccumulatorStage,
    BaseSizedChunksSource,
    BaseSizedChunksStage,
)
from .mappers import make_map

__all__ = [
    "Encode",
    "Lower",
    "StringChunksFirstSeparator",
    "StringChunksFirstSeparatorSource",
    "StringChunksMixin",
    "StringChunksSeparator",
    "StringChunksSeparatorSource",
    "StringChunksSlowStart",
    "StringChunksSlowStartSource",
    "StringDataAccumulator",
    "StringSeparatorMixin",
    "StringSizedChunks",
    "StringSizedChunksSource",
    "Upper",
]


class StringChunksMixin:
    """
    Mixin that sets the empty ``""`` buffer of the str chunk classes.

    The generic classes in :mod:`pyrill.chunks` work on ``str`` or ``bytes`` and need
    the empty buffer value of the type. This mixin supplies it as ``_buffer`` and must
    come before the generic base class in the bases. Use it to build a custom chunker
    for str data.

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseDataAccumulatorStage
        from pyrill.strings import StringChunksMixin
        from pyrill.sources import IterableSource


        class Joiner(StringChunksMixin, BaseDataAccumulatorStage[str]):
            pass


        source = IterableSource(source=["ab", "cd"])
        stage = source >> Joiner()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["abcd"]
    """

    _buffer: str = ""


class StringSeparatorMixin(StringChunksMixin):
    r"""
    Mixin that adds the default separator ``\n`` to :class:`StringChunksMixin`.

    It sets the empty buffer value and implements the abstract class method
    ``default_separator()`` required by the separator classes of
    :mod:`pyrill.chunks`. Override ``default_separator()`` in a subclass to change
    the default.

    Usage:

    .. code-block:: python

        from pyrill.chunks import BaseChunksSeparatorStage
        from pyrill.strings import StringSeparatorMixin
        from pyrill.sources import IterableSource


        class Lines(StringSeparatorMixin, BaseChunksSeparatorStage[str]):
            pass


        source = IterableSource(source=["a\nb"])
        stage = source >> Lines()

        await stage.mount()
        lines = [line async for line in stage]
        # ["a\n", "b"]
    """

    @classmethod
    def default_separator(cls) -> str:
        r"""
        Return the default separator, ``\n``.
        """
        return "\n"


# Sources


class StringSizedChunksSource(StringChunksMixin, BaseSizedChunksSource[str]):
    """
    Source that re-chunks pushed text into chunks of bounded size.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    Pushed text is accumulated in an internal buffer. A chunk is emitted as soon
    as the buffer holds more than ``min_size`` characters, and each chunk holds at
    most ``max_size`` characters. When the stream ends, any remaining text is flushed
    as a final, possibly smaller, chunk.

    :param min_size: A chunk is emitted only when the buffer holds more than this
        many characters. Values below ``1`` are raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk. Values below ``min_size``
        are raised to ``min_size``. Defaults to ``102400``.

    Input and output with ``min_size=3, max_size=4``::

        pushed:  "ab", "cdefghij", <end of stream>
        emitted: "abcd", "efgh", "ij"

    Usage:

    .. code-block:: python

        from pyrill.strings import StringSizedChunksSource

        source = StringSizedChunksSource(min_size=3, max_size=4)
        await source.mount()

        await source.push_frame("ab")
        await source.push_frame("cdefghij")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["abcd", "efgh", "ij"]
    """


class StringChunksSlowStartSource(StringChunksMixin, BaseChunksSlowStartSource[str]):
    """
    Source that re-chunks pushed text using an adaptive (slow-start) chunk size.

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
    progressively produce larger chunks. When the stream ends, any remaining text
    is flushed as a final chunk.

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

        pushed:  "abcdef", <end of stream>
        emitted: "abcd", "ef"

    Usage:

    .. code-block:: python

        from pyrill.strings import StringChunksSlowStartSource

        source = StringChunksSlowStartSource(min_size=1, max_size=4)
        await source.mount()

        await source.push_frame("abcdef")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["abcd", "ef"]
    """


class StringChunksSeparatorSource(StringSeparatorMixin, BaseChunksSeparatorSource[str]):
    r"""
    Source that splits pushed text into chunks terminated by a separator.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    Each emitted chunk ends with (and includes) the separator. A chunk may span
    several pushed frames. When the stream ends, trailing text without a
    separator is flushed as a final chunk.

    :param separator: String that terminates each chunk. Defaults to ``"\n"``.

    Input and output with the default separator::

        pushed:  "line 1\nline", " 2\npartial", <end of stream>
        emitted: "line 1\n", "line 2\n", "partial"

    Usage:

    .. code-block:: python

        from pyrill.strings import StringChunksSeparatorSource

        source = StringChunksSeparatorSource()
        await source.mount()

        await source.push_frame("line 1\nline")
        await source.push_frame(" 2\npartial")
        await source.end_of_stream()

        lines = [line async for line in source]
        # ["line 1\n", "line 2\n", "partial"]
    """


class StringChunksFirstSeparatorSource(
    StringSeparatorMixin, BaseChunksFirstSeparatorSource[str]
):
    r"""
    Source that splits off the first separator-terminated chunk of pushed text.

    Frames are fed with :meth:`push_frame`; call :meth:`end_of_stream` once all
    data has been pushed. ``push_frame`` raises :class:`RuntimeError` when the
    source is not mounted or the stream has already ended.

    The first emitted chunk holds everything up to and including the first
    separator, which suits a leading header line. After that, buffered data is
    emitted as-is, without further splitting.

    :param separator: String that terminates the first chunk. Defaults to ``"\n"``.

    Input and output with the default separator::

        pushed:  "header\nbody", " more\ndata", <end of stream>
        emitted: "header\n", "body more\ndata"

    Usage:

    .. code-block:: python

        from pyrill.strings import StringChunksFirstSeparatorSource

        source = StringChunksFirstSeparatorSource()
        await source.mount()

        await source.push_frame("header\nbody")
        await source.push_frame(" more\ndata")
        await source.end_of_stream()

        chunks = [chunk async for chunk in source]
        # ["header\n", "body more\ndata"]
    """


# Middle stages


class StringSizedChunks(StringChunksMixin, BaseSizedChunksStage[str]):
    """
    Stage that re-chunks a text stream into chunks of bounded size.

    Incoming frames are accumulated in an internal buffer. A chunk is emitted as
    soon as the buffer holds more than ``min_size`` characters, and each chunk holds
    at most ``max_size`` characters. When the upstream ends, any remaining text is
    flushed as a final, possibly smaller, chunk.

    :param min_size: A chunk is emitted only when the buffer holds more than this
        many characters. Values below ``1`` are raised to ``1``. Defaults to ``1024``.
    :param max_size: Maximum size of each emitted chunk. Values below ``min_size``
        are raised to ``min_size``. Defaults to ``102400``.

    Input and output with ``min_size=3, max_size=4``::

        input:  "ab", "cdefghij"
        output: "abcd", "efgh", "ij"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import StringSizedChunks

        source = IterableSource(source=["ab", "cdefghij"])
        stage = source >> StringSizedChunks(min_size=3, max_size=4)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abcd", "efgh", "ij"]
    """


class StringChunksSlowStart(StringChunksMixin, BaseChunksSlowStartStage[str]):
    """
    Stage that merges a text stream into chunks that grow with throughput.

    Incoming frames are buffered. When the buffer holds more than the current
    *emit threshold*, the whole buffer is emitted as one chunk, capped at
    ``max_size`` characters. The threshold controls when a chunk is emitted, not its
    size: a chunk is whatever has accumulated by then.

    The threshold starts at ``min_size`` and is recalculated on every buffer
    check, which happens once before each upstream read and once after each frame is
    appended:

    * the previous check was less than ``interval`` seconds ago and the
      threshold is below ``max_size``: the threshold is multiplied by
      ``multiplier``;
    * otherwise, if the threshold is above ``min_size``: it is divided by
      ``multiplier``.

    The new threshold is rounded to the nearest integer and kept between
    ``min_size`` and ``max_size``. On a fast stream the threshold therefore grows
    geometrically and frames are merged into progressively larger chunks, while a
    slow stream returns to small, low-latency chunks. The threshold is reset to
    ``min_size`` on unmount. When the stream ends, any remaining text is flushed as a
    final chunk. See :class:`pyrill.bytes.BytesChunksSlowStart` for a detailed
    description.

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

    Input and output with ``min_size=4, max_size=64``, when the frames arrive
    quickly::

        input:  "abcd", "efgh", "ijkl", "mnop"
        output: "abcdefghijklmnop"

    For comparison, :class:`StringSizedChunks` with ``min_size=4`` keeps a fixed
    threshold and emits ``"abcdefgh", "ijklmnop"``.

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import StringChunksSlowStart

        source = IterableSource(source=["abcd", "efgh", "ijkl", "mnop"])
        stage = source >> StringChunksSlowStart(min_size=4, max_size=64)

        await stage.mount()
        chunks = [chunk async for chunk in stage]
        # ["abcdefghijklmnop"]
    """


class StringChunksSeparator(StringSeparatorMixin, BaseChunksSeparatorStage[str]):
    r"""
    Stage that splits a text stream into chunks terminated by a separator.

    Each emitted chunk ends with (and includes) the separator, regardless of how
    the input frames are split. When the upstream ends, trailing text without a
    separator is flushed as a final chunk.

    :param separator: String that terminates each chunk. Defaults to ``"\n"``.

    Input and output with ``separator="\r\n"``::

        input:  "a\r\nb\r", "\nc"
        output: "a\r\n", "b\r\n", "c"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import StringChunksSeparator

        source = IterableSource(source=["GET / HTTP/1.1\nHost: x", "\nAccept: */*\n"])
        stage = source >> StringChunksSeparator()

        await stage.mount()
        lines = [line async for line in stage]
        # ["GET / HTTP/1.1\n", "Host: x\n", "Accept: */*\n"]
    """


class StringChunksFirstSeparator(
    StringSeparatorMixin, BaseChunksFirstSeparatorStage[str]
):
    r"""
    Stage that splits off the first separator-terminated chunk of a text stream.

    The first emitted chunk holds everything up to and including the first
    separator, which suits a leading header or status line. After that, incoming
    frames are passed through unchanged, without further splitting.

    :param separator: String that terminates the first chunk. Defaults to ``"\n"``.

    Input and output with ``separator="\r\n"``::

        input:  "HTTP/1.1 200 OK\r\nbody ", "part 1", " part 2"
        output: "HTTP/1.1 200 OK\r\n", "body ", "part 1", " part 2"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import StringChunksFirstSeparator

        source = IterableSource(source=["HTTP/1.1 200 OK\r\nbody ", "part 1", " part 2"])
        stage = source >> StringChunksFirstSeparator(separator="\r\n")

        await stage.mount()
        status_line = await stage.__anext__()
        # "HTTP/1.1 200 OK\r\n"
        body = [chunk async for chunk in stage]
        # ["body ", "part 1", " part 2"]
    """


class StringDataAccumulator(StringChunksMixin, BaseDataAccumulatorStage[str]):
    """
    Stage that joins every frame of a text stream into a single frame.

    Frames are buffered until the upstream ends, then emitted as one chunk.
    Set ``max_buffer_size`` to bound memory use; exceeding it raises
    :class:`OverflowError`.

    :param max_buffer_size: Maximum number of characters that the buffer may hold.
        ``0`` disables the limit. Defaults to ``0``.

    Input and output::

        input:  "ab", "c", "de"
        output: "abcde"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import StringDataAccumulator

        source = IterableSource(source=["ab", "c", "de"])
        stage = source >> StringDataAccumulator()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["abcde"]
    """


@make_map
def Lower(frame: str, /, **kwargs: Any) -> str:
    """
    Map stage that converts each text frame to lower case.

    It applies :meth:`str.lower` to every frame.

    Input and output::

        input:  "Hello", "WORLD"
        output: "hello", "world"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import Lower

        source = IterableSource(source=["Hello", "WORLD"])
        stage = source >> Lower()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["hello", "world"]
    """
    return frame.lower()


@make_map
def Upper(frame: str, /, **kwargs: Any) -> str:
    """
    Map stage that converts each text frame to upper case.

    It applies :meth:`str.upper` to every frame.

    Input and output::

        input:  "Hello", "world"
        output: "HELLO", "WORLD"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import Upper

        source = IterableSource(source=["Hello", "world"])
        stage = source >> Upper()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["HELLO", "WORLD"]
    """
    return frame.upper()


@make_map
def Encode(
    frame: str, /, *, encoding: str = "utf-8", errors: str = "strict", **kwargs: Any
) -> bytes:
    r"""
    Map stage that encodes each text frame into ``bytes``.

    Every frame is encoded independently with :meth:`str.encode`. It is the
    counterpart of :class:`pyrill.bytes.Decode`.

    :param encoding: Text encoding used to encode each frame. Defaults to
        ``"utf-8"``.
    :param errors: Error handling scheme, as in :meth:`str.encode`. With the default
        ``"strict"`` a character that cannot be encoded raises
        :class:`UnicodeEncodeError`. Defaults to ``"strict"``.

    Input and output with the default encoding::

        input:  "café", "naïve"
        output: b"caf\xc3\xa9", b"na\xc3\xafve"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strings import Encode

        source = IterableSource(source=["café"])
        stage = source >> Encode(encoding="latin-1")

        await stage.mount()
        frames = [frame async for frame in stage]
        # [b"caf\xe9"]
    """
    return frame.encode(encoding=encoding, errors=errors)

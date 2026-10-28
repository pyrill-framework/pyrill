import asyncio
import io
from asyncio import StreamReader, StreamReaderProtocol, StreamWriter
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pyrill.base import ElementState
from pyrill.io import (
    ChunkedFileSource,
    ChunkedIOSource,
    ChunkedStreamReaderSource,
    FileSink,
    LineIOSource,
    LineStreamReaderSource,
    StreamReaderSink,
    StreamReaderSource,
    WriteTransportSource,
)
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestChunkedIOSource
# ---------------------------------------------------------------------------


class TestChunkedIOSource:
    async def test_emits_data_in_chunks_of_specified_size(self) -> None:
        sio: io.StringIO = io.StringIO("helloworld")
        stage: ChunkedIOSource[str] = ChunkedIOSource[str](source=sio, chunk_size=5)
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == "hello"

    async def test_partial_chunk_emitted_at_end_of_stream(self) -> None:
        sio: io.StringIO = io.StringIO("hi")
        stage: ChunkedIOSource[str] = ChunkedIOSource[str](source=sio, chunk_size=5)
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == "hi"

    async def test_exhausted_source_raises_stop_async_iteration(self) -> None:
        sio: io.StringIO = io.StringIO("x")
        stage: ChunkedIOSource[str] = ChunkedIOSource[str](source=sio, chunk_size=5)
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_reads_bytes_from_binary_io(self) -> None:
        bio: io.BytesIO = io.BytesIO(b"abcde")
        stage: ChunkedIOSource[bytes] = ChunkedIOSource[bytes](source=bio, chunk_size=3)
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == b"abc"


# ---------------------------------------------------------------------------
# TestLineIOSource
# ---------------------------------------------------------------------------


class TestLineIOSource:
    async def test_emits_lines_including_newline_character(self) -> None:
        sio: io.StringIO = io.StringIO("line1\nline2\n")
        stage: LineIOSource[str] = LineIOSource[str](source=sio)
        await stage.mount()

        result = await stage.__anext__()

        assert result == "line1\n"

    async def test_emits_each_line_independently(self) -> None:
        sio: io.StringIO = io.StringIO("a\nb\n")
        stage: LineIOSource[str] = LineIOSource[str](source=sio)
        await stage.mount()
        _ = await stage.__anext__()

        second = await stage.__anext__()

        assert second == "b\n"

    async def test_exhausted_source_raises_stop_async_iteration(self) -> None:
        sio: io.StringIO = io.StringIO("only\n")
        stage: LineIOSource[str] = LineIOSource[str](source=sio)
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_reads_bytes_lines_from_binary_io(self) -> None:
        bio: io.BytesIO = io.BytesIO(b"hello\nworld\n")
        stage: LineIOSource[bytes] = LineIOSource[bytes](source=bio)
        await stage.mount()

        result = await stage.__anext__()

        assert result == b"hello\n"


# ---------------------------------------------------------------------------
# TestStreamReaderSource
# ---------------------------------------------------------------------------


class TestStreamReaderSource:
    async def test_emits_lines_from_stream(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"line1\nline2\n")
        stream.feed_eof()
        stage: StreamReaderSource = StreamReaderSource(stream=stream)
        await stage.mount()

        result = await stage.__anext__()

        assert result == b"line1\n"

    async def test_emits_successive_lines_in_order(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"first\nsecond\n")
        stream.feed_eof()
        stage: StreamReaderSource = StreamReaderSource(stream=stream)
        await stage.mount()
        _ = await stage.__anext__()

        second = await stage.__anext__()

        assert second == b"second\n"

    async def test_exhausted_stream_raises_stop_async_iteration(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"only\n")
        stream.feed_eof()
        stage: StreamReaderSource = StreamReaderSource(stream=stream)
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()


# ---------------------------------------------------------------------------
# TestChunkedStreamReaderSource
# ---------------------------------------------------------------------------


class TestChunkedStreamReaderSource:
    async def test_emits_data_in_chunks_of_specified_size(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"helloworld")
        stream.feed_eof()
        stage: ChunkedStreamReaderSource = ChunkedStreamReaderSource(
            stream=stream, chunk_size=5
        )
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == b"hello"

    async def test_exhausted_stream_raises_stop_async_iteration(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"hi")
        stream.feed_eof()
        stage: ChunkedStreamReaderSource = ChunkedStreamReaderSource(
            stream=stream, chunk_size=5
        )
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()


# ---------------------------------------------------------------------------
# TestLineStreamReaderSource
# ---------------------------------------------------------------------------


class TestLineStreamReaderSource:
    async def test_anext_at_eof_ends_stream(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"x\n")
        stream.feed_eof()
        stage: LineStreamReaderSource = LineStreamReaderSource(stream=stream)
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

        assert stage.state is ElementState.NULL

    async def test_anext_with_line_over_limit_raises(self) -> None:
        stream: StreamReader = StreamReader(limit=4)
        stream.feed_data(b"abcdefghij\n")
        stream.feed_eof()
        stage: LineStreamReaderSource = LineStreamReaderSource(stream=stream)
        await stage.mount()

        with pytest.raises(ValueError):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR

    async def test_emits_lines_from_stream(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"alpha\nbeta\n")
        stream.feed_eof()
        stage: LineStreamReaderSource = LineStreamReaderSource(stream=stream)
        await stage.mount()

        result = await stage.__anext__()

        assert result == b"alpha\n"

    async def test_emits_successive_lines_in_order(self) -> None:
        stream: StreamReader = StreamReader()
        stream.feed_data(b"x\ny\n")
        stream.feed_eof()
        stage: LineStreamReaderSource = LineStreamReaderSource(stream=stream)
        await stage.mount()
        _ = await stage.__anext__()

        second = await stage.__anext__()

        assert second == b"y\n"


# ---------------------------------------------------------------------------
# TestStreamReaderSink
# ---------------------------------------------------------------------------


class TestStreamReaderSink:
    async def test_feeds_all_frames_to_stream(self) -> None:
        stream: StreamReader = StreamReader()
        source: IterableSource[bytes] = IterableSource(source=[b"hello", b" world"])
        sink: StreamReaderSink = source >> StreamReaderSink(stream=stream)

        await sink.consume_all()

        data = await stream.read(-1)
        assert data == b"hello world"

    async def test_feeds_eof_to_stream_on_unmount(self) -> None:
        stream: StreamReader = StreamReader()
        source: IterableSource[bytes] = IterableSource(source=[b"abc"])
        sink: StreamReaderSink = source >> StreamReaderSink(stream=stream)

        await sink.consume_all()
        # Drain buffered data; read(-1) would block if feed_eof() was not called
        _ = await stream.read(-1)

        assert stream.at_eof()


# ---------------------------------------------------------------------------
# TestWriteTransportSource
# ---------------------------------------------------------------------------


class TestWriteTransportSource:
    async def test_next_frame_without_pause_does_not_resume_protocol(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)
        await source.mount()
        source.write(b"hi")

        frame = await source.__anext__()

        assert frame == b"hi"
        mock_protocol.pause_writing.assert_not_called()
        mock_protocol.resume_writing.assert_not_called()

    async def test_write_increases_buffer_size(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_protocol(MagicMock())
        await source.mount()

        source.write(b"hello")

        assert source.get_write_buffer_size() == 5

    async def test_writelines_concatenates_and_buffers(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_protocol(MagicMock())
        await source.mount()

        source.writelines([b"hello", b" world"])

        assert source.get_write_buffer_size() == 11

    async def test_get_write_buffer_limits_returns_defaults(self) -> None:
        source: WriteTransportSource = WriteTransportSource()

        result = source.get_write_buffer_limits()

        assert result == (1, 1024)

    async def test_set_write_buffer_limits_updates_high_and_low(self) -> None:
        source: WriteTransportSource = WriteTransportSource()

        source.set_write_buffer_limits(high=512, low=64)

        assert source.get_write_buffer_limits() == (64, 512)

    async def test_set_write_buffer_limits_high_only_preserves_existing_low(
        self,
    ) -> None:
        source: WriteTransportSource = WriteTransportSource()

        source.set_write_buffer_limits(high=512)

        assert source.get_write_buffer_limits() == (1, 512)

    async def test_is_closing_false_when_mounted_and_no_eos(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_protocol(MagicMock())
        await source.mount()

        result = source.is_closing()

        assert result is False

    async def test_can_write_eof_true_when_mounted_and_no_eos(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_protocol(MagicMock())
        await source.mount()

        result = source.can_write_eof()

        assert result is True

    async def test_write_eof_sets_eos_state(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_protocol(MagicMock())
        await source.mount()

        source.write_eof()

        assert source.is_closing() is True
        assert source.can_write_eof() is False

    async def test_mount_notifies_protocol_connection_made(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)

        await source.mount()

        mock_protocol.connection_made.assert_called_once_with(source)
        mock_protocol.connection_lost.assert_not_called()
        assert source.is_closing() is False

    async def test_mount_without_protocol_raises_runtime_error(self) -> None:
        source: WriteTransportSource = WriteTransportSource()

        with pytest.raises(RuntimeError, match="No protocol connected"):
            await source.mount()

    async def test_unmount_notifies_protocol_connection_lost(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)
        await source.mount()

        await source.unmount()

        mock_protocol.connection_lost.assert_called_once_with(None)
        assert source.is_closing() is True

    async def test_write_at_high_watermark_pauses_protocol(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_write_buffer_limits(high=5, low=1)
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)
        await source.mount()

        source.write(b"hello")  # exactly the high watermark

        mock_protocol.pause_writing.assert_called_once()

    async def test_write_below_high_watermark_does_not_pause_protocol(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_write_buffer_limits(high=10, low=1)
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)
        await source.mount()

        source.write(b"hello")  # 5 bytes < high watermark of 10

        mock_protocol.pause_writing.assert_not_called()

    async def test_write_below_low_watermark_does_not_drain_buffer(self) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_write_buffer_limits(high=10, low=5)
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)
        await source.mount()
        # Start _next_frame so it blocks waiting on the empty-buffer condition
        frame_task: asyncio.Task[bytes] = asyncio.create_task(source.__anext__())
        await asyncio.sleep(0)  # let _next_frame reach wait_for

        source.write(b"hi")  # 2 bytes < low=5, no drain task scheduled

        await asyncio.sleep(0)  # event loop tick — condition must not be notified
        assert not frame_task.done()
        assert source.get_write_buffer_size() == 2

        source.write(b", I'm a test")
        assert source.get_write_buffer_size() == 14
        await asyncio.sleep(0)  # event loop tick — condition must not be notified
        assert await frame_task == b"hi, I'm a test"

    async def test_next_frame_returns_buffered_data_and_resumes_protocol(
        self,
    ) -> None:
        source: WriteTransportSource = WriteTransportSource()
        source.set_write_buffer_limits(high=5, low=1)
        mock_protocol: MagicMock = MagicMock()
        source.set_protocol(mock_protocol)
        await source.mount()

        # write() hits high watermark → pause_writing(); _next_frame reads frame
        # then calls resume_writing() in its finally block
        source.write(b"hello")
        frame = await source.__anext__()

        assert frame == b"hello"
        mock_protocol.pause_writing.assert_called_once()
        mock_protocol.resume_writing.assert_called_once()


# ---------------------------------------------------------------------------
# TestChunkedFileSource
# ---------------------------------------------------------------------------


class TestChunkedFileSource:
    async def test_reads_file_in_chunks_of_specified_size(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("helloworld")
        stage: ChunkedFileSource[str] = ChunkedFileSource[str](
            path=f, mode="r", chunk_size=5
        )
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == "hello"

    async def test_partial_last_chunk_emitted(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("hi")
        stage: ChunkedFileSource[str] = ChunkedFileSource[str](
            path=f, mode="r", chunk_size=5
        )
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == "hi"

    async def test_exhausted_file_raises_stop_async_iteration(
        self, tmp_path: Path
    ) -> None:
        f = tmp_path / "data.txt"
        f.write_text("x")
        stage: ChunkedFileSource[str] = ChunkedFileSource[str](
            path=f, mode="r", chunk_size=5
        )
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_reads_bytes_from_binary_file(self, tmp_path: Path) -> None:
        f = tmp_path / "data.bin"
        f.write_bytes(b"\x00\x01\x02\x03\x04")
        stage: ChunkedFileSource[bytes] = ChunkedFileSource[bytes](
            path=f, mode="rb", chunk_size=3
        )
        await stage.mount()

        chunk = await stage.__anext__()

        assert chunk == b"\x00\x01\x02"


# ---------------------------------------------------------------------------
# TestFileSink
# ---------------------------------------------------------------------------


class TestFileSink:
    async def test_writes_text_frames_to_file(self, tmp_path: Path) -> None:
        f = tmp_path / "out.txt"
        source: IterableSource[str] = IterableSource(source=["hello\n", "world\n"])
        sink: FileSink[str] = source >> FileSink[str](path=f)

        await sink.consume_all()

        assert f.read_text() == "hello\nworld\n"

    async def test_writes_bytes_frames_to_binary_file(self, tmp_path: Path) -> None:
        f = tmp_path / "out.bin"
        source: IterableSource[bytes] = IterableSource(source=[b"\x01\x02", b"\x03"])
        sink: FileSink[bytes] = source >> FileSink[bytes](path=f, mode="wb")

        await sink.consume_all()

        assert f.read_bytes() == b"\x01\x02\x03"

    async def test_multiple_frames_written_in_order(self, tmp_path: Path) -> None:
        f = tmp_path / "out.txt"
        source: IterableSource[str] = IterableSource(source=["a", "b", "c"])
        sink: FileSink[str] = source >> FileSink[str](path=f)

        await sink.consume_all()

        assert f.read_text() == "abc"


# ---------------------------------------------------------------------------
# TestWriteTransportSourceWithStreamWriter
# ---------------------------------------------------------------------------


def _make_stream_writer(transport: WriteTransportSource) -> StreamWriter:
    """Wrap an unmounted WriteTransportSource; mount() calls connection_made()."""
    reader: StreamReader = StreamReader()
    protocol: StreamReaderProtocol = StreamReaderProtocol(reader)
    transport.set_protocol(protocol)
    return StreamWriter(transport, protocol, reader, asyncio.get_event_loop())


class TestWriteTransportSourceWithStreamWriter:
    async def test_writer_write_below_high_watermark_flows_as_frame(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()
        writer.write(b"hi")

        frame = await transport.__anext__()

        assert frame == b"hi"
        assert transport.state is ElementState.READY

    async def test_writer_write_flows_through_transport_as_frame(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        transport.set_write_buffer_limits(high=5, low=1)
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        # write() hits the high watermark → pause_writing() on protocol + orphan drain
        # scheduled to notify the _next_frame condition. resume_writing() is called
        # in _next_frame's finally after the frame is read.
        writer.write(b"hello")
        frame = await transport.__anext__()

        assert frame == b"hello"

    async def test_writer_writelines_concatenates_into_single_frame(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        transport.set_write_buffer_limits(high=5, low=1)
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        writer.writelines([b"ab", b"cde"])
        frame = await transport.__anext__()

        assert frame == b"abcde"

    async def test_drain_blocks_until_frame_is_consumed(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        transport.set_write_buffer_limits(high=4, low=1)
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        # write() hits the high watermark → pause_writing() (_paused=True) + orphan
        # drain. frame_task must be created before awaiting writer.drain() so both
        # can run concurrently: drain() blocks while _paused=True and unblocks when
        # _next_frame calls resume_writing() in its finally block after reading.
        writer.write(b"data")
        frame_task: asyncio.Task[bytes] = asyncio.create_task(transport.__anext__())
        await writer.drain()
        frame = await frame_task

        assert frame == b"data"

    async def test_drain_does_not_return_before_buffer_is_consumed(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        transport.set_write_buffer_limits(high=4, low=1)
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        writer.write(b"data")
        frame_task: asyncio.Task[bytes] = asyncio.create_task(transport.__anext__())

        await writer.drain()

        # drain() unblocks only when resume_writing() is called, which _next_frame
        # calls in its finally block after reading the buffer.  frame_task therefore
        # must already be done — and carrying the correct result — the moment
        # drain() returns.
        assert frame_task.done()
        assert frame_task.result() == b"data"

    async def test_writer_can_write_eof_delegates_to_transport(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        result = writer.can_write_eof()

        assert result is True

    async def test_writer_write_eof_disables_further_writes(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        writer.write_eof()

        assert writer.can_write_eof() is False

    async def test_writer_is_closing_reflects_transport_state(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        result = writer.transport.is_closing()

        assert result is False

    async def test_multiple_writes_each_produce_a_frame(self) -> None:
        transport: WriteTransportSource = WriteTransportSource()
        transport.set_write_buffer_limits(high=3, low=1)
        writer: StreamWriter = _make_stream_writer(transport)
        await transport.mount()

        writer.write(b"abc")
        first = await transport.__anext__()

        writer.write(b"xyz")
        second = await transport.__anext__()

        assert first == b"abc"
        assert second == b"xyz"

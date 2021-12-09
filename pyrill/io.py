from asyncio import Condition, Future, IncompleteReadError
from pathlib import Path
from typing import IO, AnyStr, Dict, Generic, Iterable, Optional, TypeVar

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

from .base import BaseSink, BaseSource

__all__ = ['MSG_NEW_FILE', 'MSG_FILE_PATH', 'MSG_FILE_SIZE', 'MSG_START_FILE',
           'MSG_FILE_READ_START', 'MSG_FILE_READ_END', 'IOSourceMixin', 'IOSinkMixin',
           'SyncIOSink', 'AsyncIOSink', 'StreamReadSink', 'StreamWriterSource',
           'ChunkedIOSourceMixin', 'ChunkedSyncIOSource', 'ChunkedAsyncIOSource',
           'SyncReadLineIOSource', 'AsyncReadLineIOSource', 'FileSource', 'FileSink']

MSG_FILE_SIZE = 'file-size'
MSG_FILE_PATH = 'file-path'
MSG_NEW_FILE = 'new-file'
MSG_START_FILE = 'start-file'
MSG_FILE_READ_START = 'file-read-start'
MSG_FILE_READ_END = 'file-read-end'


class SyncReadStreamProtocol(Protocol[AnyStr]):

    def read(self, n: int = -1) -> AnyStr:
        pass


class SyncReadLineStreamProtocol(Protocol[AnyStr]):

    def readline(self) -> AnyStr:
        pass


class SyncWriteStreamProtocol(Protocol[AnyStr]):

    def write(self, data: AnyStr) -> int:
        pass


class AsyncReadStreamProtocol(Protocol[AnyStr]):

    async def read(self, n: int = -1) -> AnyStr:
        pass


class AsyncReadLineStreamProtocol(Protocol[AnyStr]):

    async def readline(self) -> AnyStr:
        pass


class AsyncWriteStreamProtocol(Protocol[AnyStr]):

    async def write(self, data: AnyStr) -> int:
        pass

    async def drain(self):
        pass

    async def wait_closed(self):
        pass


ReadStreamProtocol_t = TypeVar('ReadStreamProtocol_t',
                               SyncReadStreamProtocol,
                               SyncReadLineStreamProtocol,
                               AsyncReadStreamProtocol,
                               AsyncReadLineStreamProtocol)

WriteStreamProtocol_t = TypeVar('WriteStreamProtocol_t',
                                SyncWriteStreamProtocol,
                                AsyncWriteStreamProtocol)


class IOSourceMixin(Generic[ReadStreamProtocol_t]):
    def __init__(self, *args, source: ReadStreamProtocol_t, **kwargs):
        super(IOSourceMixin, self).__init__(*args, **kwargs)

        self._source = source


class ChunkedIOSourceMixin:

    def __init__(self, *args, chunk_size: int = 1024, **kwargs):
        super(ChunkedIOSourceMixin, self).__init__(*args, **kwargs)

        self._chunk_size = chunk_size


class ChunkedSyncIOSource(ChunkedIOSourceMixin,
                          IOSourceMixin[SyncReadStreamProtocol[AnyStr]],
                          BaseSource[AnyStr]):
    async def _next_frame(self) -> AnyStr:
        data = self._source.read(self._chunk_size)
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class ChunkedAsyncIOSource(ChunkedIOSourceMixin,
                           IOSourceMixin[AsyncReadStreamProtocol[AnyStr]],
                           BaseSource[AnyStr]):
    async def _next_frame(self) -> AnyStr:
        data = await self._source.read(self._chunk_size)
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class SyncReadLineIOSource(IOSourceMixin[SyncReadLineStreamProtocol[AnyStr]],
                           BaseSource[AnyStr]):

    async def _next_frame(self) -> AnyStr:
        data = self._source.readline()
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class AsyncReadLineIOSource(IOSourceMixin[AsyncReadLineStreamProtocol[AnyStr]],
                            BaseSource[AnyStr]):

    async def _next_frame(self) -> AnyStr:
        data = await self._source.readline()
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class StreamWriterSource(BaseSource[AnyStr]):
    transport = None

    def __init__(self, *args, **kwargs):
        super(StreamWriterSource, self).__init__(*args, **kwargs)

        self._condition = Condition()
        self._buffer = b''
        self._closed_fut: Optional[Future] = None

    def write(self, data: AnyStr):
        if self._closed_fut is not None:
            raise RuntimeError('Stream already closed')
        self._buffer += data
        self._drain()

    def writelines(self, lines: Iterable[AnyStr]):
        if self._closed_fut is not None:
            raise RuntimeError('Stream already closed')
        for data in lines:
            self.write(data)

    def close(self):
        if self._closed_fut is not None:
            raise RuntimeError('Stream already closed')
        self._closed_fut = Future()
        self._drain()

    def can_write_eof(self):
        return self._closed_fut is None

    def write_eof(self):
        if not self.can_write_eof():
            raise RuntimeError('EOF already written')
        self.close()

    def is_closing(self):
        return self._closed_fut is not None and not self._closed_fut.done()

    def is_closed(self):
        return self._closed_fut is not None and self._closed_fut.done()

    async def wait_closed(self):
        if self._closed_fut is None:
            self.close()
        return await self._closed_fut

    def _drain(self):
        self._condition.notify(1)

    async def drain(self):
        self._drain()

    def get_extra_info(self, name: str, default=None):
        return default

    async def _next_frame(self) -> bytes:
        while True:
            if len(self._buffer):
                result = self._buffer
                self._buffer = self._buffer[:0]
                return result
            if self.is_closing():
                self._closed_fut.set_result(True)
                raise StopAsyncIteration()
            if self.is_closed():
                raise StopAsyncIteration()


class FileSource(ChunkedIOSourceMixin, BaseSource[AnyStr]):

    def __init__(self, *args, path: Path, mode: str = 'r', **kwargs):
        super(FileSource, self).__init__(*args, **kwargs)

        self._path = path
        self._mode = mode
        self._stream: Optional[IO] = None
        self._start_read = True

    async def _mount(self):
        self.bus.send_message(self._build_message(MSG_NEW_FILE,
                                                  path=self._path,
                                                  size=self._path.stat().st_size))
        self._stream = self._path.open(mode=self._mode)
        self._start_read = True

        await super(FileSource, self)._mount()

    async def _unmount(self):
        self._stream.close()
        self.bus.send_message(self._build_message(MSG_FILE_READ_END,
                                                  path=self._path))

    async def _next_frame(self) -> AnyStr:
        if self._start_read:
            self._start_read = False

            self.bus.send_message(self._build_message(MSG_FILE_READ_START,
                                                      path=self._path))

        data = self._stream.read(self._chunk_size)
        if len(data) == 0:
            raise StopAsyncIteration()
        return data


class IOSinkMixin(Generic[WriteStreamProtocol_t]):
    def __init__(self, *args, sink: WriteStreamProtocol_t, **kwargs):
        super(IOSinkMixin, self).__init__(*args, **kwargs)

        self._sink = sink


class SyncIOSink(IOSinkMixin[SyncWriteStreamProtocol[AnyStr]], BaseSink[AnyStr]):

    async def _consume_frame(self) -> AnyStr:
        data = await super(SyncIOSink, self)._consume_frame()
        self._sink.write(data)

        return self._sink


class AsyncIOSink(IOSinkMixin[AsyncWriteStreamProtocol[AnyStr]], BaseSink[AnyStr]):

    async def _unmount(self):
        self._sink.write_eof()
        self._sink.close()
        await self._sink.wait_closed()
        await super(AsyncIOSink, self)._unmount()

    async def _consume_frame(self) -> AnyStr:
        data = await super(AsyncIOSink, self)._consume_frame()
        await self._sink.write(data)
        try:
            await self._sink.drain()
        except AttributeError:
            pass
        return self._sink


class StreamReadSink(BaseSink[bytes]):
    transport = None

    def __init__(self, *args, **kwargs):
        super(StreamReadSink, self).__init__(*args, **kwargs)

        self._buffer = b''
        self._at_eof = False

    async def _mount(self):
        self._buffer = b''
        self._at_eof = False

        await super(StreamReadSink, self)._mount()

    async def _unmount(self):
        self._buffer = b''
        self._at_eof = False

        await super(StreamReadSink, self)._unmount()

    async def read(self, n: int = -1) -> bytes:
        while True:
            if len(self._buffer) >= n >= 0:
                data = self._buffer[:n]
                self._buffer = self._buffer[n:]
                return data
            else:
                try:
                    self._buffer += await self.consume_frame()
                except StopAsyncIteration:
                    data = self._buffer
                    self._buffer = b''
                    self._at_eof = True
                    return data

    async def readuntil(self, separator=b'\n') -> bytes:
        while True:
            if separator in self._buffer:
                return await self.read(self._buffer.index(separator))
            else:
                try:
                    await self.consume_frame()
                except StopAsyncIteration:
                    return await self.read(-1)

    async def readline(self) -> bytes:
        return await self.readuntil(b'\n')

    async def readexactly(self, n: int) -> bytes:
        data = await self.read(n)
        if len(data) < n:
            raise IncompleteReadError(data, n)
        return data

    def at_eof(self):
        return self._at_eof


class FileSink(BaseSink[AnyStr]):
    def __init__(self, *args, path: Path, mode: str = 'w', open_kwargs: Dict = None, **kwargs):
        super(FileSink, self).__init__(*args, **kwargs)

        self._path = path
        self._mode = mode
        self._open_kwargs = open_kwargs or {}
        self._sink: Optional[IO] = None

    async def _mount(self):
        self._sink = self._path.open(mode=self._mode, **self._open_kwargs)

        await super(FileSink, self)._mount()

    async def _consume_frame(self) -> AnyStr:
        try:
            data = await super(FileSink, self).consume_frame()
        except StopAsyncIteration:
            self._sink.close()
            raise

        self._sink.write(data)
        return data

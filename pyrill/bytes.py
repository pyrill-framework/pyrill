from typing import Optional

from . import BaseStage
from .base import BaseIndependentConsumerStage, BaseSource, FrameSkippedError
from .chunks import (BaseChunksFirstSeparatorProducer,
                     BaseChunksSeparatorProducer, BaseChunksSlowStartProducer,
                     BaseSizedChunksProducer)
from .mappers import make_map

__all__ = ['BytesSizedChunksSource', 'BytesChunksSlowStartSource', 'BytesChunksSeparatorSource',
           'BytesChunksFirstSeparatorSource', 'BytesSizedChunks', 'BytesChunksSlowStart', 'BytesChunksSeparator',
           'BytesChunksFirstSeparator', 'Decode', 'UnicodeChunks']


class BytesChunksMixin:
    @classmethod
    def empty_buffer(cls) -> bytes:
        return b''


class BytesSeparatorMixin(BytesChunksMixin):
    @classmethod
    def default_separator(cls) -> bytes:
        return b'\n'


# Sources

class BytesSizedChunksSource(BytesChunksMixin, BaseSizedChunksProducer[bytes], BaseSource[bytes]):
    pass


class BytesChunksSlowStartSource(BytesChunksMixin, BaseChunksSlowStartProducer[bytes], BaseSource[bytes]):
    pass


class BytesChunksSeparatorSource(BytesSeparatorMixin, BaseChunksSeparatorProducer[bytes], BaseSource[bytes]):
    pass


class BytesChunksFirstSeparatorSource(BytesSeparatorMixin, BaseChunksFirstSeparatorProducer[bytes], BaseSource[bytes]):
    pass


# Middle stages

class BytesSizedChunks(BytesChunksMixin,
                       BaseSizedChunksProducer[bytes],
                       BaseIndependentConsumerStage[bytes]):
    pass


class BytesChunksSlowStart(BytesChunksMixin,
                           BaseChunksSlowStartProducer[bytes],
                           BaseIndependentConsumerStage[bytes]):
    pass


class BytesChunksSeparator(BytesSeparatorMixin,
                           BaseChunksSeparatorProducer[bytes],
                           BaseIndependentConsumerStage[bytes]):
    pass


class BytesChunksFirstSeparator(BytesSeparatorMixin,
                                BaseChunksFirstSeparatorProducer[bytes],
                                BaseIndependentConsumerStage[bytes]):
    pass


@make_map
def Decode(frame: bytes, *, encoding: str = 'utf-8', **kwargs) -> str:
    return frame.decode(encoding=encoding)


def _get_valid_unicode_bytes(data: bytes, encoding='utf-8') -> bytes:
    while len(data):
        try:
            data.decode(encoding)
        except UnicodeDecodeError:
            data = data[:-1]
        else:
            return data

    return data


class UnicodeChunks(BaseStage[bytes, bytes]):
    _buffer: Optional[bytes] = None
    _open_stream: bool = False

    def __init__(self, *args, encoding='utf-8', **kwargs):
        super(UnicodeChunks, self).__init__(*args, **kwargs)

        self.encoding = encoding

    async def _mount(self):
        self._buffer = None
        self._open_stream = True
        await super(UnicodeChunks, self)._mount()

    async def _unmount(self):
        self._buffer = None
        self._open_stream = False
        await super(UnicodeChunks, self)._unmount()

    async def process_frame(self, frame: bytes) -> bytes:
        if self._buffer is None:
            self._buffer = frame
        else:
            self._buffer += frame

        if len(self._buffer) == 0:
            if not self._open_stream:
                raise StopAsyncIteration()
            else:
                raise FrameSkippedError()

        result = _get_valid_unicode_bytes(self._buffer)

        if len(result) == 0:
            if not self._open_stream:
                self._buffer = self._buffer[:0]
                raise StopAsyncIteration()
            else:
                raise FrameSkippedError()

        self._buffer = self._buffer[len(result):]

        return result

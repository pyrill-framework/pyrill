from .base import BaseIndependentConsumerStage, BaseSource, FrameSkippedError
from .chunks import (BaseChunksFirstSeparatorProducer,
                     BaseChunksSeparatorProducer, BaseChunksSlowStartProducer,
                     BaseDataAccumulatorProducer, BaseSizedChunksProducer)
from .mappers import make_map

__all__ = ['SizedChunksSource', 'ChunksSlowStartSource', 'ChunksSeparatorSource', 'ChunksFirstSeparatorSource',
           'SizedChunks', 'ChunksSlowStart', 'ChunksSeparator', 'ChunksFirstSeparator', 'Decode',
           'UnicodeChunks']


class BytesChunksMixin:
    @classmethod
    def empty_buffer(cls) -> bytes:
        return b''


class BytesSeparatorMixin(BytesChunksMixin):
    @classmethod
    def default_separator(cls) -> bytes:
        return b'\n'


# Sources

class SizedChunksSource(BytesChunksMixin, BaseSizedChunksProducer[bytes], BaseSource[bytes]):
    pass


class ChunksSlowStartSource(BytesChunksMixin, BaseChunksSlowStartProducer[bytes], BaseSource[bytes]):
    pass


class ChunksSeparatorSource(BytesSeparatorMixin, BaseChunksSeparatorProducer[bytes], BaseSource[bytes]):
    pass


class ChunksFirstSeparatorSource(BytesSeparatorMixin, BaseChunksFirstSeparatorProducer[bytes], BaseSource[bytes]):
    pass


# Middle stages

class SizedChunks(BytesChunksMixin,
                  BaseSizedChunksProducer[bytes],
                  BaseIndependentConsumerStage[bytes]):
    pass


class ChunksSlowStart(BytesChunksMixin,
                      BaseChunksSlowStartProducer[bytes],
                      BaseIndependentConsumerStage[bytes]):
    pass


class ChunksSeparator(BytesSeparatorMixin,
                      BaseChunksSeparatorProducer[bytes],
                      BaseIndependentConsumerStage[bytes]):
    pass


class ChunksFirstSeparator(BytesSeparatorMixin,
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


class UnicodeChunks(BytesChunksMixin,
                    BaseDataAccumulatorProducer[bytes],
                    BaseIndependentConsumerStage[bytes]):

    def __init__(self, *args, encoding='utf-8', **kwargs):
        super(UnicodeChunks, self).__init__(*args, **kwargs)

        self.encoding = encoding

    async def _next_chunk(self) -> bytes:
        if self._buffer is None:
            self._buffer = self.empty_buffer()

        if len(self._buffer) == 0:
            raise FrameSkippedError()

        result = _get_valid_unicode_bytes(self._buffer)

        if len(result) == 0:
            raise FrameSkippedError()

        self._buffer = self._buffer[len(result):]

        return result

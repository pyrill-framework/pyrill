from .base import BaseIndependentConsumerStage, BaseSource
from .chunks import (BaseChunksFirstSeparatorProducer,
                     BaseChunksSeparatorProducer, BaseChunksSlowStartProducer,
                     BaseSizedChunksProducer)
from .mappers import make_map

__all__ = ['SizedChunksSource', 'ChunksSlowStartSource', 'ChunksSeparatorSource', 'ChunksFirstSeparatorSource',
           'SizedChunks', 'ChunksSlowStart', 'ChunksSeparator', 'ChunksFirstSeparator', 'Encode',
           'Lower', 'Upper']


class StringChunksMixin:
    @classmethod
    def empty_buffer(cls) -> str:
        return ''


class StringSeparatorMixin(StringChunksMixin):
    @classmethod
    def default_separator(cls) -> str:
        return '\n'


# Sources

class SizedChunksSource(StringChunksMixin, BaseSizedChunksProducer[str], BaseSource[str]):
    pass


class ChunksSlowStartSource(StringChunksMixin, BaseChunksSlowStartProducer[str], BaseSource[str]):
    pass


class ChunksSeparatorSource(StringSeparatorMixin, BaseChunksSeparatorProducer[str], BaseSource[str]):
    pass


class ChunksFirstSeparatorSource(StringSeparatorMixin, BaseChunksFirstSeparatorProducer[str], BaseSource[str]):
    pass


# Middle stages

class SizedChunks(StringChunksMixin,
                  BaseSizedChunksProducer[str],
                  BaseIndependentConsumerStage[str]):
    pass


class ChunksSlowStart(StringChunksMixin,
                      BaseChunksSlowStartProducer[str],
                      BaseIndependentConsumerStage[str]):
    pass


class ChunksSeparator(StringSeparatorMixin,
                      BaseChunksSeparatorProducer[str],
                      BaseIndependentConsumerStage[str]):
    pass


class ChunksFirstSeparator(StringSeparatorMixin,
                           BaseChunksFirstSeparatorProducer[str],
                           BaseIndependentConsumerStage[str]):
    pass


@make_map
def Lower(frame: str, **kwargs) -> str:
    return frame.lower()


@make_map
def Upper(frame: str, **kwargs) -> str:
    return frame.upper()


@make_map
def Encode(frame: str, *, encoding: str = 'utf-8', errors: str = 'strict', **kwargs) -> bytes:
    return frame.encode(encoding=encoding, errors=errors)

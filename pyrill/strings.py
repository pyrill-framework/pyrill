from .base import BaseIndependentConsumerStage, BaseSource
from .chunks import (BaseChunksFirstSeparatorProducer,
                     BaseChunksSeparatorProducer, BaseChunksSlowStartProducer,
                     BaseSizedChunksProducer)
from .mappers import make_map

__all__ = ['StringSizedChunksSource', 'StringChunksSlowStartSource', 'StringChunksSeparatorSource',
           'StringChunksFirstSeparatorSource',
           'StringSizedChunks', 'StringChunksSlowStart', 'StringChunksSeparator', 'StringChunksFirstSeparator',
           'Encode', 'Lower', 'Upper']


class StringChunksMixin:
    @classmethod
    def empty_buffer(cls) -> str:
        return ''


class StringSeparatorMixin(StringChunksMixin):
    @classmethod
    def default_separator(cls) -> str:
        return '\n'


# Sources

class StringSizedChunksSource(StringChunksMixin, BaseSizedChunksProducer[str], BaseSource[str]):
    pass


class StringChunksSlowStartSource(StringChunksMixin, BaseChunksSlowStartProducer[str], BaseSource[str]):
    pass


class StringChunksSeparatorSource(StringSeparatorMixin, BaseChunksSeparatorProducer[str], BaseSource[str]):
    pass


class StringChunksFirstSeparatorSource(StringSeparatorMixin, BaseChunksFirstSeparatorProducer[str], BaseSource[str]):
    pass


# Middle stages

class StringSizedChunks(StringChunksMixin,
                        BaseSizedChunksProducer[str],
                        BaseIndependentConsumerStage[str]):
    pass


class StringChunksSlowStart(StringChunksMixin,
                            BaseChunksSlowStartProducer[str],
                            BaseIndependentConsumerStage[str]):
    pass


class StringChunksSeparator(StringSeparatorMixin,
                            BaseChunksSeparatorProducer[str],
                            BaseIndependentConsumerStage[str]):
    pass


class StringChunksFirstSeparator(StringSeparatorMixin,
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

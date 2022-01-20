from .base import BaseIndependentConsumerStage, BaseSource
from .chunks import (BaseChunksFirstSeparatorProducer,
                     BaseChunksSeparatorProducer, BaseChunksSlowStartProducer,
                     BaseDataChunkProducerIndependentConsumerMixin,
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
                        BaseDataChunkProducerIndependentConsumerMixin[str],
                        BaseSizedChunksProducer[str],
                        BaseIndependentConsumerStage[str]):
    pass


class StringChunksSlowStart(StringChunksMixin,
                            BaseDataChunkProducerIndependentConsumerMixin[str],
                            BaseChunksSlowStartProducer[str],
                            BaseIndependentConsumerStage[str]):
    pass


class StringChunksSeparator(StringSeparatorMixin,
                            BaseDataChunkProducerIndependentConsumerMixin[str],
                            BaseChunksSeparatorProducer[str],
                            BaseIndependentConsumerStage[str]):

    async def _consume_frame(self) -> str:
        try:
            return await super(StringChunksSeparator, self)._consume_frame()
        except StopAsyncIteration:
            self._open_buffer = False
            await self._notify()
            raise


class StringChunksFirstSeparator(StringSeparatorMixin,
                                 BaseDataChunkProducerIndependentConsumerMixin[str],
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

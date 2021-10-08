from abc import ABC, abstractmethod
from asyncio import CancelledError, Condition, Future
from datetime import datetime
from typing import AnyStr, Optional, Union

from .base import (BaseIndependentConsumer, BaseIndependentConsumerStage,
                   BaseProducer, FrameSkippedError)


class BaseDataChunkProducer(BaseProducer[AnyStr], ABC):
    _buffer: Optional[AnyStr] = None

    def __init__(self, *args, **kwargs):
        super(BaseDataChunkProducer, self).__init__(*args, **kwargs)

        self._open_buffer = False
        self._condition = Condition(loop=self._loop)

    @classmethod
    @abstractmethod
    def empty_buffer(cls) -> AnyStr:  # pragma: nocover
        raise NotImplementedError()

    async def _mount(self):
        self._buffer = self.empty_buffer()
        self._open_buffer = True
        await super(BaseDataChunkProducer, self)._mount()

    async def _unmount(self):
        self._buffer = None
        self._open_buffer = False
        await super(BaseDataChunkProducer, self)._unmount()

    async def push_frame(self, frame: Union[AnyStr, BaseException]):
        if not self._open_buffer:
            raise RuntimeError('Stream already finished')
        if self._buffer is None:
            raise RuntimeError('Buffer not ready')

        if isinstance(frame, StopAsyncIteration):
            self._open_buffer = False
        elif isinstance(frame, BaseException):
            pass
        else:
            self._buffer += frame

        await self._notify()

    async def _notify(self):
        if len(self._buffer) or not self._open_buffer:
            async with self._condition:
                self._condition.notify()

    async def _next_frame(self) -> AnyStr:
        while True:
            try:
                return await self._next_chunk()
            except FrameSkippedError:
                pass

            async with self._condition:
                await self._condition.wait()

    @abstractmethod
    async def _next_chunk(self) -> AnyStr:
        raise NotImplementedError()


class BaseDataAccumulatorProducer(BaseDataChunkProducer[AnyStr], ABC):
    async def _next_chunk(self) -> AnyStr:
        if self._buffer is None:
            raise RuntimeError('Buffer not initialized')
        if len(self._buffer) == 0:
            raise FrameSkippedError()

        result = self._buffer
        self._buffer = self.empty_buffer()
        return result


class BaseSizedChunksProducer(BaseDataChunkProducer[AnyStr], ABC):

    def __init__(self, *args, min_size: int = 0, max_size: int = 1024, **kwargs):
        super(BaseSizedChunksProducer, self).__init__(*args, **kwargs)

        self._max_size = max_size
        self._min_size = min_size

    async def _notify(self):
        if len(self._buffer) >= self._min_size:
            await super(BaseSizedChunksProducer, self)._notify()

    async def _next_chunk(self) -> AnyStr:
        if self._buffer is None:
            raise RuntimeError('Buffer not initialized')

        if len(self._buffer) < self._min_size and self._open_buffer:
            raise FrameSkippedError()
        if len(self._buffer) == 0 and not self._open_buffer:
            raise StopAsyncIteration()

        result = self._buffer[:self._max_size]
        self._buffer = self._buffer[len(result):]
        await self._notify()
        return result


class BaseChunksSlowStartProducer(BaseSizedChunksProducer[AnyStr], ABC):

    def __init__(self,
                 *args,
                 interval: float = 1.0,
                 multiplier: float = 2.,
                 **kwargs):
        super(BaseChunksSlowStartProducer, self).__init__(*args, **kwargs)

        self._interval = max(interval, 0.001)
        self._multiplier = max(multiplier, 1.)

        self._initial_min_size = self._min_size
        self._last_dt: Optional[datetime] = None

    async def _unmount(self):
        self._last_dt = None
        await super(BaseChunksSlowStartProducer, self)._unmount()

    def _calculate_chunk_size(self):
        if self._last_dt is None:
            self._min_size = self._initial_min_size
        else:
            if (datetime.now() - self._last_dt).total_seconds() < self._interval and self._min_size < self._max_size:
                self._min_size = round(self._min_size * self._multiplier)
            elif self._min_size > self._initial_min_size:
                self._min_size = round(self._min_size / self._multiplier)

            self._min_size = max(min(self._min_size, self._max_size), self._initial_min_size)

        self._last_dt = datetime.now()

    async def _next_chunk(self) -> AnyStr:
        self._calculate_chunk_size()
        return await super(BaseChunksSlowStartProducer, self)._next_chunk()


class BaseChunksSeparatorProducer(BaseDataAccumulatorProducer[AnyStr], ABC):

    def __init__(self, *args, separator: AnyStr = None, **kwargs):
        super(BaseChunksSeparatorProducer, self).__init__(*args, **kwargs)

        self.separator: AnyStr = separator or self.default_separator()

    @classmethod
    @abstractmethod
    def default_separator(cls) -> AnyStr:  # pragma: nocover
        raise NotImplementedError()

    async def _notify(self):
        if not self._open_buffer or self.separator in self._buffer:
            await super(BaseChunksSeparatorProducer, self)._notify()
            return

    async def _next_chunk(self) -> AnyStr:
        if self._buffer is None:
            raise RuntimeError('Buffer not initialized')

        if self.separator not in self._buffer and self._open_buffer:
            raise FrameSkippedError()
        result = self._buffer.split(self.separator, 1)
        try:
            self._buffer = result[1]
        except IndexError:
            self._buffer = self._buffer[:0]

        await self._notify()
        return result[0] + self.separator


class BaseChunksFirstSeparatorProducer(BaseChunksSeparatorProducer[AnyStr], ABC):
    _first_sep = False

    async def _mount(self):
        self._first_sep = True

        await super(BaseChunksFirstSeparatorProducer, self)._mount()

    async def _notify(self):
        if not self._first_sep or not self._open_buffer or self.separator in self._buffer:
            await super(BaseChunksSeparatorProducer, self)._notify()
            return

    async def _next_chunk(self) -> AnyStr:
        if self._buffer is None:
            raise RuntimeError('Buffer not initialized')

        if ((self._first_sep and self.separator not in self._buffer) or len(self._buffer) == 0) and self._open_buffer:
            raise FrameSkippedError()
        if self._first_sep:
            result = await super(BaseChunksFirstSeparatorProducer, self)._next_chunk()
            self._first_sep = False
        else:
            result = self._buffer
            self._buffer = self._buffer[:0]
        return result

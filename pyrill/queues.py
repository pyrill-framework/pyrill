from abc import ABC
from asyncio import Queue as AsyncQueue
from typing import Optional, Union

from .base import (BaseIndependentConsumerStage, BaseProducer, BaseSource,
                   Source_co)


class BaseQueue(BaseProducer[Source_co], ABC):
    _queue: 'Optional[AsyncQueue[Union[Source_co, BaseException]]]' = None

    def __init__(self, *args, queue_size: int = 0, **kwargs):
        super(BaseQueue, self).__init__(*args, **kwargs)

        self._queue_size = queue_size
        self._open_queue = False

    async def _mount(self):
        self._queue = AsyncQueue(maxsize=self._queue_size)
        self._open_queue = True
        await super(BaseQueue, self)._mount()

    async def _unmount(self):
        self._queue = None
        self._open_queue = False
        await super(BaseQueue, self)._unmount()

    async def push_frame(self, frame: Union[Source_co, BaseException]):
        if not self._open_queue:
            raise RuntimeError('Stream already finished')
        if self._queue is None:
            raise RuntimeError('Queue not ready')

        if isinstance(frame, StopAsyncIteration):
            self._open_queue = False

        await self._queue.put(frame)

    async def _next_frame(self) -> Source_co:
        frame = await self._queue.get()

        if isinstance(frame, BaseException):
            raise frame
        return frame


class Queue(BaseQueue[Source_co], BaseIndependentConsumerStage[Source_co]):
    async def _next_frame(self) -> Source_co:
        if self._consumer_fut is None:
            self.start_consumer()
        return await super(Queue, self)._next_frame()

    async def _inner_consume_all(self):
        await self.mount()

        if self._consumer_fut is None:
            return

        try:
            await self._consumer_fut
        except StopAsyncIteration:
            self.log('Sink finished')
            return
        except Exception as ex:
            await self._set_error(ex)
            raise


class QueueSource(BaseQueue[Source_co], BaseSource[Source_co]):
    pass

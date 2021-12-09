from abc import ABC
from asyncio import Future, ensure_future, gather
from typing import Optional, Union, cast

from .base import BaseSink, FrameSkippedError, Sink_co, Source_co

__all__ = ['BaseOneFrameSink', 'Last', 'First', 'BlackHole']


class BaseOneFrameSink(BaseSink[Sink_co], ABC):
    _frame_fut: 'Optional[Future[Sink_co]]' = None

    async def _mount(self):
        self._frame_fut = Future()

        await super(BaseOneFrameSink, self)._mount()

    async def _unmount(self):
        if not self._frame_fut.done():
            self._frame_fut.cancel()
        ensure_future(gather(self._frame_fut, return_exceptions=True))

        await super(BaseOneFrameSink, self)._unmount()

    def reset_frame(self):
        self._frame_fut = None

    async def get_frame(self) -> Sink_co:
        if self._frame_fut is not None:
            return await self._frame_fut
        await self.mount()
        self.consume_all()
        if self._frame_fut is None:
            raise RuntimeError('Consumer not initialized')
        return await self._frame_fut


class First(BaseOneFrameSink[Sink_co]):
    async def _consume_frame(self) -> None:  # type: ignore[override]
        if self._frame_fut is None:
            raise RuntimeError('Consumer not initialized')
        if self._iter is None:
            raise RuntimeError('Iterator not initialized')
        try:
            frame = await self._iter.__anext__()
            self._frame_fut.set_result(frame)
            raise StopAsyncIteration()
        except StopAsyncIteration:
            if not self._frame_fut.done():
                self._frame_fut.set_exception(ValueError())
            raise


class Last(BaseOneFrameSink[Sink_co]):
    EMPTY = object()

    _previous_frame: 'Union[Sink_co, object]' = EMPTY

    async def _mount(self):
        self._previous_frame = self.EMPTY

        await super(Last, self)._mount()

    async def _unmount(self):
        self._previous_frame = self.EMPTY

        await super(Last, self)._unmount()

    async def _consume_frame(self) -> Sink_co:
        if self._frame_fut is None:
            raise RuntimeError('Consumer not initialized')
        if self._iter is None:
            raise RuntimeError('Iterator not initialized')
        try:
            frame = await self._iter.__anext__()
            self._previous_frame = frame
            raise FrameSkippedError()
        except StopAsyncIteration:
            if not self._frame_fut.done():
                if self._previous_frame is self.EMPTY:
                    self._frame_fut.set_exception(ValueError())
                else:
                    self._frame_fut.set_result(cast(Sink_co, self._previous_frame))
                    return cast(Sink_co, self._previous_frame)
            raise
        except FrameSkippedError:
            raise
        except BaseException as ex:
            self._frame_fut.set_exception(ex)
            raise


class BlackHole(BaseSink[Source_co]):
    pass

from . import FrameSkippedError, Noop
from .base import Source_co


class BreakpointFrame(Noop[Source_co]):  # pragma: no cover

    async def process_frame(self, frame: Source_co) -> Source_co:
        breakpoint()
        return frame


class BreakpointEnd(Noop[Source_co]):  # pragma: no cover

    async def _next_frame(self) -> Source_co:
        try:
            return super(BreakpointEnd, self)._next_frame()
        except StopAsyncIteration:
            breakpoint()
            raise


class BreakpointOnSkip(Noop[Source_co]):  # pragma: no cover

    async def _next_frame(self) -> Source_co:
        try:
            return super(BreakpointEnd, self)._next_frame()
        except FrameSkippedError as ex:  # noqa
            breakpoint()
            raise

from .base import FrameSkipped
from .primitives import Noop


class BreakpointFrame[Source_co](Noop[Source_co]):  # pragma: no cover
    """
    Stage that calls ``breakpoint()`` on every frame and then passes it through.

    Use it for interactive debugging: insert it anywhere in a chain to inspect each
    frame in the debugger. Frames are not modified. The example below is a plain literal
    block because it stops in the debugger, so the example checker skips it.

    Input and output::

        input:  "a", "b"
        output: "a", "b"  (the debugger stops before each frame is emitted)

    Usage::

        from pyrill.debug import BreakpointFrame
        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b"])
        stage = source >> BreakpointFrame()

        await stage.mount()
        frames = [frame async for frame in stage]
    """

    async def _process_frame(self, frame: Source_co) -> Source_co:
        breakpoint()  # noqa: T100
        return frame


class BreakpointEnd[Source_co](Noop[Source_co]):  # pragma: no cover
    """
    Stage that calls ``breakpoint()`` when its source ends, then lets the end continue.

    Use it for interactive debugging, to inspect the state of the chain right before the
    end of the stream. Frames pass through unchanged. The example below is a plain
    literal block because it stops in the debugger, so the example checker skips it.

    Input and output::

        input:  "a", "b", <end of stream>
        output: "a", "b"  (the debugger stops when the end of stream is reached)

    Usage::

        from pyrill.debug import BreakpointEnd
        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b"])
        stage = source >> BreakpointEnd()

        await stage.mount()
        frames = [frame async for frame in stage]
    """

    async def _next_frame(self) -> Source_co:
        try:
            return await super()._next_frame()
        except StopAsyncIteration:
            breakpoint()  # noqa: T100
            raise


class BreakpointOnSkip[Source_co](Noop[Source_co]):  # pragma: no cover
    """
    Stage that calls ``breakpoint()`` when a frame is skipped upstream.

    Use it for interactive debugging, to find out where frames are dropped: when the
    source raises :class:`~pyrill.base.FrameSkipped`, the debugger starts and the skip
    is then re-raised. Frames that are not skipped pass through unchanged. The example
    below is a plain literal block because it stops in the debugger, so the example
    checker skips it.

    Input and output::

        input:  "a", <skipped frame>, "b"
        output: "a", "b"  (the debugger stops when the skip happens)

    Usage::

        from pyrill.debug import BreakpointOnSkip
        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b"])
        stage = source >> BreakpointOnSkip()

        await stage.mount()
        frames = [frame async for frame in stage]
    """

    async def _next_frame(self) -> Source_co:
        try:
            return await super()._next_frame()
        except FrameSkipped:
            breakpoint()  # noqa: T100
            raise

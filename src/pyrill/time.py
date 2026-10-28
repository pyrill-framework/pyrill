from asyncio import sleep, timeout
from datetime import datetime
from typing import Any

from pyrill.primitives import Noop

__all__ = [
    "Delayed",
    "StartAt",
    "StopAt",
    "Timeout",
]


class Delayed[Source_co](Noop[Source_co]):
    """
    Stage that waits ``delay`` seconds before emitting each frame.

    Frames are not modified. The wait happens once per frame, after the frame is
    received. An empty stream produces no wait.

    :param delay: Seconds to sleep before emitting each frame.

    Input and output::

        input:  "a", "b"        (each frame is held for ``delay`` seconds)
        output: "a", "b"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.time import Delayed

        source = IterableSource(source=["a", "b"])
        stage = source >> Delayed(delay=0.01)

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["a", "b"]
    """

    def __init__(self, *args: Any, delay: float, **kwargs: Any) -> None:
        self.delay = delay
        super().__init__(*args, **kwargs)

    async def _process_frame(self, frame: Source_co) -> Source_co:
        await sleep(self.delay)
        return await super()._process_frame(frame)


class Timeout[Source_co](Noop[Source_co]):
    """
    Stage that fails when a frame takes too long to arrive.

    The limit applies to each frame separately: the stage waits at most ``timeout``
    seconds for its source to deliver the next frame. When it expires, the stage raises
    :class:`TimeoutError` and goes to ``ERROR``. The end of the stream is not a timeout.

    :param timeout: Maximum seconds to wait for each frame.

    Input and output::

        input:  "a", <nothing for longer than ``timeout``>
        output: "a", then TimeoutError

    Usage:

    .. code-block:: python

        from contextlib import suppress

        from pyrill.queues import QueueSource
        from pyrill.time import Timeout

        source = QueueSource()
        stage = source >> Timeout(timeout=0.05)

        await stage.mount()
        await source.push_frame("a")
        first = await stage.__anext__()
        with suppress(TimeoutError):
            await stage.__anext__()
        state = stage.state.name
        # "ERROR"
    """

    def __init__(self, *args: Any, timeout: float, **kwargs: Any) -> None:
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    async def consume_frame(self) -> Source_co:
        """
        Get the next frame from the source, waiting at most ``timeout`` seconds.
        """
        async with timeout(self.timeout):
            return await super().consume_frame()


class StopAt[Source_co](Noop[Source_co]):
    """
    Stage that raises :class:`TimeoutError` once a deadline is reached.

    Frames pass through unchanged until ``stop_at``. If the deadline has already passed
    when a frame is requested, or it is reached while the stage waits for a frame, the
    stage raises :class:`TimeoutError` and goes to ``ERROR``. Naive and timezone-aware
    datetimes both work: the current time is read in the timezone of ``stop_at``.

    :param stop_at: Moment from which the stage stops, as a ``datetime``.

    Input and output::

        input:  "a", "b", ...   (frames before ``stop_at``)
        output: "a", "b", ..., then TimeoutError at ``stop_at``

    Usage:

    .. code-block:: python

        from contextlib import suppress
        from datetime import UTC, datetime, timedelta

        from pyrill.queues import QueueSource
        from pyrill.time import StopAt

        source = QueueSource()
        stage = source >> StopAt(stop_at=datetime.now(UTC) + timedelta(seconds=0.05))

        await stage.mount()
        await source.push_frame("a")
        first = await stage.__anext__()
        with suppress(TimeoutError):
            await stage.__anext__()
        state = stage.state.name
        # "ERROR"
    """

    def __init__(self, *args: Any, stop_at: datetime, **kwargs: Any) -> None:
        self.stop_at = stop_at
        super().__init__(*args, **kwargs)

    async def _next_frame(self) -> Source_co:
        now = datetime.now(self.stop_at.tzinfo)
        if now >= self.stop_at:
            raise TimeoutError()

        async with timeout(max(0.0, (self.stop_at - now).total_seconds())):
            return await super()._next_frame()


class StartAt[Source_co](Noop[Source_co]):
    """
    Stage that holds the first frame until a start time.

    Before each frame is requested, if ``start_at`` is still in the future, the stage
    sleeps until then. Once the time has passed, frames flow without any delay. Naive
    and timezone-aware datetimes both work: the current time is read in the timezone of
    ``start_at``.

    :param start_at: Moment from which frames may flow, as a ``datetime``.

    Input and output::

        input:  "a", "b"        (the first frame waits until ``start_at``)
        output: "a", "b"

    Usage:

    .. code-block:: python

        from datetime import UTC, datetime, timedelta

        from pyrill.sources import IterableSource
        from pyrill.time import StartAt

        start_at = datetime.now(UTC) + timedelta(seconds=0.05)
        source = IterableSource(source=["a", "b"])
        stage = source >> StartAt(start_at=start_at)

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["a", "b"]
    """

    def __init__(self, *args: Any, start_at: datetime, **kwargs: Any) -> None:
        self.start_at = start_at
        super().__init__(*args, **kwargs)

    async def _next_frame(self) -> Source_co:
        now = datetime.now(self.start_at.tzinfo)
        if now < self.start_at:
            await sleep((self.start_at - now).total_seconds())
        return await super()._next_frame()

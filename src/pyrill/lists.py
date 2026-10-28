from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from .base import BaseStage

__all__ = [
    "FlatIterable",
    "GetItem",
    "GroupFrames",
]


class FlatIterable[Source_co](BaseStage[Iterable[Source_co], Source_co]):
    """
    Stage that flattens a stream of iterables into a stream of their items.

    Each incoming frame must be iterable. Its items are emitted one by one, in order,
    before the next frame is consumed. Empty iterables produce no output. The stream
    ends when the upstream ends and the last iterable has been exhausted.

    Input and output::

        input:  [1, 2], [], [3]
        output: 1, 2, 3

    Usage:

    .. code-block:: python

        from pyrill.lists import FlatIterable
        from pyrill.sources import IterableSource

        source = IterableSource(source=[[1, 2], [], [3]])
        stage = source >> FlatIterable()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3]
    """

    _inner_iter: Iterator[Source_co] | None = None

    async def _mount(self) -> None:
        self._inner_iter = None

        await super()._mount()

    async def _unmount(self) -> None:
        self._inner_iter = None

        await super()._unmount()

    async def _next_frame(self) -> Source_co:
        while True:
            if self._inner_iter is None:
                self._inner_iter = iter(await self.consume_frame())

            try:
                return self._inner_iter.__next__()
            except StopIteration:
                self._inner_iter = None
                continue

    async def _process_frame(  # pragma: no cover
        self, frame: Iterable[Source_co]
    ) -> Source_co:
        raise NotImplementedError()


class GroupFrames[Source_co](BaseStage[Source_co, list[Source_co]]):
    """
    Stage that groups consecutive frames into lists.

    Frames are collected until the list holds ``max_length`` frames, then the list is
    emitted. When the upstream ends, the remaining frames are emitted as a final,
    possibly shorter, list. No empty list is ever emitted.

    :param max_length: Maximum number of frames per list. ``0`` (the default) means
        no limit: all frames are grouped into a single list emitted at the end of the
        stream.

    Input and output with ``max_length=2``::

        input:  1, 2, 3, 4, 5
        output: [1, 2], [3, 4], [5]

    Usage:

    .. code-block:: python

        from pyrill.lists import GroupFrames
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3, 4, 5])
        stage = source >> GroupFrames(max_length=2)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [[1, 2], [3, 4], [5]]
    """

    def __init__(self, *args: Any, max_length: int = 0, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.max_length = max_length

    async def _next_frame(self) -> list[Source_co]:
        buffer: list[Source_co] = []
        try:
            while self.max_length == 0 or len(buffer) < self.max_length:
                buffer.append(await self.consume_frame())
        except StopAsyncIteration:
            if not buffer:
                raise
        return buffer

    async def _process_frame(  # pragma: no cover
        self, frame: Source_co
    ) -> list[Source_co]:
        raise NotImplementedError()


class GetItem[Source_co](BaseStage[Sequence[Source_co], Source_co]):
    """
    Stage that emits one item of each incoming sequence.

    Each frame must support indexing. The item at ``index`` is emitted. Negative
    indexes count from the end. A frame that is too short raises
    :class:`IndexError`, which puts the element in the error state.

    :param index: Position of the item to extract. Defaults to ``0``.

    Input and output with ``index=1``::

        input:  ["a", "b"], ["c", "d", "e"]
        output: "b", "d"

    Usage:

    .. code-block:: python

        from pyrill.lists import GetItem
        from pyrill.sources import IterableSource

        source = IterableSource(source=[["a", "b"], ["c", "d", "e"]])
        stage = source >> GetItem(index=1)

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["b", "d"]
    """

    def __init__(self, *args: Any, index: int = 0, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.index = index

    async def _process_frame(self, frame: Sequence[Source_co]) -> Source_co:
        return frame[self.index]

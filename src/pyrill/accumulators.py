from collections.abc import Sized
from typing import Any

from .base import BaseStage

__all__ = ["Count", "ListAcc", "Size", "SumAcc"]


class SumAcc[AccData: (str, bytes, int, complex, float)](BaseStage[AccData, AccData]):
    """
    Stage that emits the running sum of the frames received so far.

    After every frame the stage emits the accumulated value, so a source of ``n``
    frames produces ``n`` outputs. The sum uses ``+``, so it also concatenates ``str``
    and ``bytes`` frames. The accumulator is reset to ``initial_value`` on every mount
    and cleared on unmount.

    :param initial_value: Value the sum starts from. When ``None`` (default), the first
        frame becomes the initial value.

    Input and output::

        input:  1, 2, 3
        output: 1, 3, 6

    Usage:

    .. code-block:: python

        from pyrill.accumulators import SumAcc
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        stage = source >> SumAcc(initial_value=10)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [11, 13, 16]
    """

    def __init__(self, *args: Any, initial_value: AccData | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.initial_value: AccData | None = initial_value

        self._accum: AccData | None = None

    async def _mount(self) -> None:
        self._accum = self.initial_value
        await super()._mount()

    async def _unmount(self) -> None:
        self._accum = None
        await super()._unmount()

    async def _process_frame(self, frame: AccData) -> AccData:
        if self._accum is None:
            self._accum = frame
        else:
            self._accum += frame
        return self._accum


class ListAcc[Source_co](BaseStage[Source_co, list[Source_co]]):
    """
    Stage that emits the list of all the frames received so far.

    After every frame the stage emits a new list (a copy, so earlier outputs are not
    modified later) holding every frame seen since the stage was mounted. The list is
    reset on every mount.

    Input and output::

        input:  "a", "b", "c"
        output: ["a"], ["a", "b"], ["a", "b", "c"]

    Usage:

    .. code-block:: python

        from pyrill.accumulators import ListAcc
        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b", "c"])
        stage = source >> ListAcc()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [["a"], ["a", "b"], ["a", "b", "c"]]
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._accum: list[Source_co] = []

    async def _mount(self) -> None:
        self._accum = []
        await super()._mount()

    async def _process_frame(self, frame: Source_co) -> list[Source_co]:
        self._accum.append(frame)

        return self._accum.copy()


class Count(BaseStage[Any, int]):
    """
    Stage that emits how many frames it has received so far.

    The frame content is ignored. After every frame the stage emits the number of frames
    seen since it was mounted, starting at ``1``. The counter is reset on mount and on
    unmount.

    Input and output::

        input:  "a", "b", "c"
        output: 1, 2, 3

    Usage:

    .. code-block:: python

        from pyrill.accumulators import Count
        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b", "c"])
        stage = source >> Count()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3]
    """

    _counter: int = 0

    async def _mount(self) -> None:
        self._counter = 0
        await super()._mount()

    async def _unmount(self) -> None:
        self._counter = 0
        await super()._unmount()

    async def _process_frame(self, frame: Any) -> int:
        self._counter += 1
        return self._counter


class Size(BaseStage[Sized, int]):
    """
    Stage that emits the total ``len`` of the frames received so far.

    Frames can be any sized object (``str``, ``bytes``, ``list``...). After every frame
    the stage emits the sum of the lengths seen since it was mounted. The total is reset
    on mount and on unmount.

    Input and output::

        input:  "ab", "cde", "f"
        output: 2, 5, 6

    Usage:

    .. code-block:: python

        from pyrill.accumulators import Size
        from pyrill.sources import IterableSource

        source = IterableSource(source=["ab", "cde", "f"])
        stage = source >> Size()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [2, 5, 6]
    """

    _acc: int = 0

    async def _mount(self) -> None:
        self._acc = 0
        await super()._mount()

    async def _unmount(self) -> None:
        self._acc = 0
        await super()._unmount()

    async def _process_frame(self, frame: Sized) -> int:
        self._acc += len(frame)
        return self._acc

from typing import Any, List, Optional, Sized, TypeVar

from .base import BaseStage, Source_co

__all__ = ['SumAcc', 'ListAcc', 'Count', 'Size']

AccData = TypeVar('AccData', str, bytes, int, complex, float, list)


class SumAcc(BaseStage[AccData, AccData]):

    def __init__(self, *args, initial_value: AccData = None, **kwargs):
        super(SumAcc, self).__init__(*args, **kwargs)

        self.initial_value: Optional[AccData] = initial_value

        self._accum: Optional[AccData] = None

    async def _mount(self):
        self._accum = self.initial_value
        await super(SumAcc, self)._mount()

    async def _unmount(self):
        self._accum = None
        await super(SumAcc, self)._unmount()

    async def process_frame(self, frame: AccData) -> AccData:
        if self._accum is None:
            self._accum = frame
        else:
            self._accum += frame
        return self._accum


class ListAcc(BaseStage[Source_co, List[Source_co]]):

    def __init__(self, *args, **kwargs):
        super(ListAcc, self).__init__(*args, **kwargs)

        self._accum: Optional[List[Source_co]] = None

    async def _mount(self):
        self._accum = []
        await super(ListAcc, self)._mount()

    async def _unmount(self):
        self._accum = None
        await super(ListAcc, self)._unmount()

    async def process_frame(self, frame: Source_co) -> List[Source_co]:
        if self._accum is None:  # pragma: no cover
            raise RuntimeError('Accumulator not initialized')
        self._accum.append(frame)

        return self._accum.copy()


class Count(BaseStage[Any, int]):
    _counter: int = 0

    async def _mount(self):
        self._counter = 0
        await super(Count, self)._mount()

    async def _unmount(self):
        self._counter = 0
        await super(Count, self)._unmount()

    async def process_frame(self, frame: Any) -> int:
        self._counter += 1
        return self._counter


class Size(BaseStage[Sized, int]):
    _acc: int = 0

    async def _mount(self):
        self._acc = 0
        await super(Size, self)._mount()

    async def _unmount(self):
        self._acc = 0
        await super(Size, self)._unmount()

    async def process_frame(self, frame: Sized) -> int:
        self._acc += len(frame)
        return self._acc

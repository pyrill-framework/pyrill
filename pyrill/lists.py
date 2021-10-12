from typing import Iterable, Iterator, List, Optional, Sequence

from .base import BaseStage, Source_co

__all__ = ['Explode', 'Implode', 'GetItem']


class Explode(BaseStage[Iterable[Source_co], Source_co]):
    _inner_iter: Optional[Iterator[Source_co]] = None

    async def _mount(self):
        self._inner_iter = None

        await super(Explode, self)._mount()

    async def _unmount(self):
        self._inner_iter = None

        await super(Explode, self)._unmount()

    async def _next_frame(self) -> Source_co:
        while True:
            if self._inner_iter is None:
                self._inner_iter = iter(await self.consume_frame())

            try:
                return self._inner_iter.__next__()
            except StopIteration:
                self._inner_iter = None
                continue

    async def process_frame(self, frame: Iterator[Source_co]) -> Source_co:  # pragma: no cover
        raise NotImplementedError()


class Implode(BaseStage[Source_co, List[Source_co]]):

    def __init__(self, *args, max_length: int = 0, **kwargs):
        super(Implode, self).__init__(*args, **kwargs)
        self.max_length = max_length
        self._finished = False

    async def _consume_frame(self) -> List[Source_co]:
        buffer = []

        try:
            if self._finished:
                raise StopAsyncIteration()
            while len(buffer) < self.max_length:
                buffer.append(await super(Implode, self)._consume_frame())
            result = buffer
            buffer = []

            return result
        except StopAsyncIteration:
            if len(buffer) > 0:
                self._finished = True
                return buffer
            raise

    async def _mount(self):
        self._finished = False
        await super(Implode, self)._mount()

    async def process_frame(self, frame: Iterator[Source_co]) -> Source_co:
        return frame


class GetItem(BaseStage[Sequence[Source_co], Source_co]):

    def __init__(self, *args, index: int = 0, **kwargs):
        super(GetItem, self).__init__(*args, **kwargs)
        self.index = index

    async def process_frame(self, frame: Sequence[Source_co]) -> Source_co:
        return frame[self.index]

from typing import AsyncIterable, AsyncIterator, Iterable, Iterator, Optional

from .base import BaseSource, Source_co

__all__ = ['SyncSource', 'AsyncSource']


class SyncSource(BaseSource[Source_co]):
    _source: Optional[Iterable[Source_co]] = None
    _iter: Optional[Iterator[Source_co]] = None

    def __init__(self, *args, source: Iterable[Source_co] = None, **kwargs):
        super(SyncSource, self).__init__(*args, **kwargs)

        self.source = source

    @property
    def source(self) -> Optional[Iterable[Source_co]]:
        return self._source

    @source.setter
    def source(self, value: Optional[Iterable[Source_co]] = None):
        self._source = value

    async def _mount(self):
        if self._source is None:
            raise RuntimeError('Not source set')

        self._iter = iter(self._source)
        await super(SyncSource, self)._mount()

    async def _unmount(self):
        self._iter = None

        await super(SyncSource, self)._unmount()

    async def _next_frame(self) -> Source_co:
        try:
            return self._iter.__next__()
        except StopIteration:
            raise StopAsyncIteration()


class AsyncSource(BaseSource[Source_co]):
    _source: Optional[AsyncIterable[Source_co]] = None
    _iter: Optional[AsyncIterator[Source_co]] = None

    def __init__(self, *args, source: AsyncIterable[Source_co] = None, **kwargs):
        super(AsyncSource, self).__init__(*args, **kwargs)

        self.source = source

    @property
    def source(self) -> Optional[AsyncIterable[Source_co]]:
        return self._source

    @source.setter
    def source(self, value: Optional[AsyncIterable[Source_co]] = None):
        self._source = value

    async def _mount(self):
        if self._source is None:
            raise RuntimeError('Not source set')

        self._iter = self._source.__aiter__()
        await super(AsyncSource, self)._mount()

    async def _unmount(self):
        self._iter = None

        await super(AsyncSource, self)._unmount()

    async def _next_frame(self) -> Source_co:
        try:
            return await self._iter.__anext__()
        except StopAsyncIteration:
            raise

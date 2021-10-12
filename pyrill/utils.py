from asyncio import AbstractEventLoop, Future, ensure_future, get_event_loop
from typing import TYPE_CHECKING, Iterable, Iterator

if TYPE_CHECKING:
    from .base import BaseSink

__all__ = ['SinkConsumer']


class SinkConsumer:

    def __init__(self, sinks: 'Iterable[BaseSink]' = None, *, loop: 'AbstractEventLoop' = None):
        self._sinks = {}
        self._loop = loop or get_event_loop()
        self._active_fut = Future()

        if sinks:
            [self.add_sink(s) for s in sinks]

    def add_sink(self, sink: 'BaseSink'):
        if self._active_fut.done():
            raise RuntimeError('Sink consumer finished')

        if sink in self._sinks:
            return

        sink.consume_all()
        fut = self._sinks[sink] = ensure_future(sink.wait_until_eos(), loop=self._loop)
        fut.add_done_callback(lambda fut: self._notify_finish())

    def remove_sink(self, sink: 'BaseSink'):
        try:
            del self._sinks[sink]
        except KeyError:
            pass

    def _notify_finish(self):
        if self._active_fut.done():
            return

        if all([f.done() for f in self._sinks.values()]):
            self._active_fut.set_result(None)

    async def wait_until_finish_all(self):
        await self._active_fut

    def __iter__(self) -> 'Iterator[BaseSink]':
        return iter(list(self._sinks.keys()))

    def __len__(self) -> int:
        return len(self._sinks)

    def __await__(self):
        return self.wait_until_finish_all().__await__()

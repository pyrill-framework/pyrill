from typing import TYPE_CHECKING

from .base import BaseSink
from .queues import QueueSource

if TYPE_CHECKING:
    from .base import Bus, Message

__all__ = ['BusMessageSource', 'BusMessageSink']


class BusMessageSource(QueueSource['Message']):

    def __init__(self, *args, **kwargs):
        super(BusMessageSource, self).__init__(*args, **kwargs)

        self._src_bus_finished = False

    async def push_message(self, message: 'Message'):
        try:
            await self.push_frame(message)
        except RuntimeError:
            pass

    async def end_of_bus(self):
        self._src_bus_finished = True
        try:
            await self.push_frame(StopAsyncIteration())
        except RuntimeError:
            pass

    async def _mount(self):
        if self._src_bus_finished:
            raise RuntimeError('Source bus already finished')

        await super(BusMessageSource, self)._mount()


class BusMessageSink(BaseSink['Message']):

    def __init__(self, *args, src_bus: 'Bus', dst_bus: 'Bus', **kwargs):
        super(BusMessageSink, self).__init__(*args, **kwargs)

        self.src_bus = src_bus
        self.dst_bus = dst_bus

    async def _consume_frame(self) -> 'Message':
        msg = await super(BusMessageSink, self)._consume_frame()
        self.dst_bus.send_message(msg)
        return msg

    async def _unmount(self):
        self.dst_bus.unpipe(self.src_bus)

        await super(BusMessageSink, self)._unmount()

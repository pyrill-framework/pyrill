from abc import ABC, abstractmethod
from asyncio import (AbstractEventLoop, CancelledError, Future, Task,
                     ensure_future, get_event_loop)
from asyncio.locks import Lock
from dataclasses import dataclass, field
from enum import Enum
from logging import DEBUG, INFO
from typing import (TYPE_CHECKING, Any, AsyncContextManager, AsyncIterator,
                    Callable, Dict, Generic, Iterable, List, Optional, Set,
                    Tuple, TypeVar, Union, cast)
from uuid import uuid4
from weakref import WeakSet

try:
    from asyncio import current_task
except ImportError:
    current_task = Task.current_task
try:
    from contextlib import AsyncExitStack
except ImportError:
    from async_exit_stack import AsyncExitStack

__all__ = ['Message', 'ElementState', 'FrameSkippedError', 'EndOfStream',
           'BaseElement', 'BaseProducer', 'BaseConsumer', 'BaseSource', 'BaseStage', 'BaseSink',
           'BaseIndependentConsumer', 'BaseIndependentConsumerStage', 'Bus', 'MessageFilter', 'SimpleMessageFilter',
           'BUS_MSG_LOG', 'BUS_MSG_ELEMENT_NULL', 'BUS_MSG_ELEMENT_READY', 'BUS_MSG_ELEMENT_ERROR',
           'BUS_MSG_FRAME_SKIPPED', 'BUS_MSG_START_STREAM', 'BUS_MSG_STOP_STREAM', 'BUS_MSG_RESET_STREAM']

if TYPE_CHECKING:
    from .bus import BusMessageSink, BusMessageSource

Source_co = TypeVar('Source_co')
Sink_co = TypeVar('Sink_co')
LastElement = TypeVar('LastElement', bound='BaseElement')

MessageHandler = Callable[['Message'], Any]


class FrameSkippedError(BaseException):
    def __init__(self, frame: Any = None):
        self.frame = frame


class EndOfStream(StopAsyncIteration):
    pass


BUS_MSG_START_STREAM = 'start-stream'
BUS_MSG_STOP_STREAM = 'stop-stream'
BUS_MSG_RESET_STREAM = 'reset-stream'
BUS_MSG_FRAME_SKIPPED = 'frame-skipped'
BUS_MSG_LOG = 'log'
BUS_MSG_ELEMENT_READY = 'element-ready'
BUS_MSG_ELEMENT_NULL = 'element-null'
BUS_MSG_ELEMENT_ERROR = 'element-error'


@dataclass()
class Message:
    sender: str
    type: str
    params: Dict[str, Any] = field(default_factory=dict)


class ElementState(Enum):
    NULL = 'null'
    READY = 'ready'
    ERROR = 'error'


class BaseElement(ABC):

    def __init__(self, name: str = None, *, loop: 'AbstractEventLoop' = None):
        self._loop = loop or get_event_loop()

        self.name = name or self.new_name()

        self._prepared = False

        self._state = ElementState.NULL
        self._state_lock = Lock()
        self._last_error: Optional[BaseException] = None

    @property
    @abstractmethod
    def bus(self) -> Optional['Bus']:
        raise NotImplementedError()

    @property
    def state(self) -> ElementState:
        return self._state

    @classmethod
    def new_name(cls):
        return '-'.join([cls.__name__,
                         str(uuid4())])

    def __rshift__(self, other: 'BaseElement') -> 'BaseElement':
        raise TypeError(f'{other.name} can not be followed by {self.name}')

    def __lshift__(self, other: 'BaseElement') -> 'BaseElement':
        return other.__rshift__(self)

    def _build_message(self, type: str, **params):
        return Message(sender=self.name, type=type, params=params)

    def _send_message(self, msg: 'Message'):
        if self.bus is None:
            raise RuntimeError('Bus not initiated')
        self.bus.send_message(msg)

    async def _mount(self):
        pass

    async def _unmount(self):
        pass

    async def _set_error(self, ex: BaseException):
        async with self._state_lock:
            if self._state == ElementState.ERROR:
                return
            self.log('Setting error', lvl=DEBUG)
            try:
                await self._unmount()
            except Exception:
                pass
            finally:
                self._state = ElementState.ERROR
                self._last_error = ex
                self.log('Error set', lvl=DEBUG)
                try:
                    self._send_message(self._build_message(BUS_MSG_ELEMENT_ERROR, ex=ex))
                except Exception:
                    pass

    async def mount(self):
        if self._state == ElementState.ERROR:
            raise RuntimeError('Element has an error')
        try:
            async with self._state_lock:
                if self._state != ElementState.NULL:
                    return
                self.log('Mounting', lvl=DEBUG)
                await self._mount()
                self._state = ElementState.READY
                self.log('Ready', lvl=DEBUG)
                self._send_message(self._build_message(BUS_MSG_ELEMENT_READY))
        except BaseException as ex:
            await self._set_error(ex)
            raise

    async def unmount(self):
        try:
            async with self._state_lock:
                if self._state != ElementState.READY:
                    return
                self.log('Unmounting', lvl=DEBUG)
                await self._unmount()
                self._state = ElementState.NULL
                self.log('Null state', lvl=DEBUG)
                self._send_message(self._build_message(BUS_MSG_ELEMENT_NULL))
        except BaseException as ex:
            await self._set_error(ex)
            raise

    def log(self, msg, *, lvl=INFO, **kwargs):
        if lvl < INFO:
            return
        try:
            self._send_message(self._build_message(BUS_MSG_LOG, message=msg, level=lvl, **kwargs))
        except Exception:
            pass

    def __del__(self):
        try:
            if not self._loop.is_closed():
                ensure_future(self.unmount(), loop=self._loop)
        except RuntimeError:
            pass


class BaseProducer(AsyncIterator[Source_co], BaseElement, ABC):

    def __init__(self, *args, **kwargs):
        super(BaseProducer, self).__init__(*args, **kwargs)

        self._active_tasks: Set[Task] = set()

    async def __anext__(self) -> Source_co:
        ct = current_task()
        if ct is not None:
            self._active_tasks.add(ct)
        try:
            while True:
                try:
                    if self._state is not ElementState.READY:
                        await self.mount()

                    frame = await self._next_frame()
                    self.log(msg=f'Consumed frame: {frame}', lvl=DEBUG)
                    return frame
                except StopAsyncIteration:
                    self.log(msg='Stream finished', lvl=DEBUG)
                    if ct is not None:
                        self._active_tasks.remove(ct)
                    await self.unmount()
                    raise
                except FrameSkippedError as ex:
                    self._send_message(self._build_message(BUS_MSG_FRAME_SKIPPED, frame=ex.frame))
                    continue
                except BaseException as ex:
                    try:
                        self._send_message(self._build_message(BUS_MSG_ELEMENT_ERROR, ex=ex))
                    except Exception:
                        pass
                    raise
        finally:
            try:
                if ct is not None:
                    self._active_tasks.remove(ct)
            except KeyError:
                pass

    @abstractmethod
    async def _next_frame(self) -> Source_co:  # pragma: nocover
        raise NotImplementedError()

    async def _unmount(self):
        for task in self._active_tasks:
            if not task.done() and task != current_task():
                task.cancel('Unmounted element')

        await super(BaseProducer, self)._unmount()

    def __aiter__(self) -> AsyncIterator[Source_co]:
        return self

    def __rshift__(self, other: 'BaseElement') -> 'BaseElement':
        if isinstance(other, BaseConsumer):
            return other.__lshift__(self)
        else:
            return super(BaseProducer, self).__rshift__(other)


class BaseConsumer(Generic[Sink_co], BaseElement):
    _source: Optional[BaseProducer[Sink_co]] = None
    _iter: Optional[AsyncIterator[Sink_co]] = None

    def __init__(self, *args, source: BaseProducer[Sink_co] = None, **kwargs):
        super(BaseConsumer, self).__init__(*args, **kwargs)

        self.source = source

    @property
    def bus(self) -> Optional['Bus']:
        if self._source is None:
            return None
        return self._source.bus

    @property
    def source(self) -> Optional[BaseProducer[Sink_co]]:
        return self._source

    @source.setter
    def source(self, value: Optional[BaseProducer[Sink_co]] = None):
        self.set_source(value)

    def set_source(self, value: Optional[BaseProducer[Sink_co]] = None):
        if self._source == value:
            return
        if self.state == ElementState.READY:
            raise RuntimeError('Unmount before change source')
        self._source = value

    async def _mount(self):
        if self._source is None:
            raise RuntimeError('Not source set')

        await self._source.mount()
        self._iter = self._source.__aiter__()
        await super(BaseConsumer, self)._mount()

    async def _unmount(self):
        self._iter = None

        try:
            await self._source.unmount()
        except Exception:
            pass
        await super(BaseConsumer, self)._unmount()

    async def _consume_frame(self) -> Sink_co:
        if self._iter is None:
            raise RuntimeError('Iterator has not been initiate')

        return await self._iter.__anext__()

    async def consume_frame(self) -> Sink_co:
        if self._state is not ElementState.READY:
            raise RuntimeError('Not ready')
        while True:
            try:
                return await self._consume_frame()
            except FrameSkippedError as ex:
                self._send_message(self._build_message(BUS_MSG_FRAME_SKIPPED, frame=ex.frame))
                continue

    def __lshift__(self, other: 'BaseElement') -> 'BaseElement':
        if not isinstance(other, BaseProducer):
            return super(BaseConsumer, self).__lshift__(other)
        self.source = other
        return self


class BaseIndependentConsumer(BaseConsumer[Sink_co]):

    def __init__(self,
                 *args,
                 context_managers: Iterable[AsyncContextManager] = None,
                 **kwargs):
        super(BaseIndependentConsumer, self).__init__(*args, **kwargs)

        self._consumer_fut: Optional[Future] = None
        self._context_managers = tuple(context_managers) if context_managers else tuple()

    async def _consume_all(self):
        await self.mount()
        try:
            async with AsyncExitStack() as cm:
                [await cm.enter_async_context(c) for c in self._context_managers]

                while True:
                    await self.consume_frame()
        except StopAsyncIteration:
            self.log('Consumer finished')
        except Exception as ex:
            await self._set_error(ex)
            raise
        finally:
            ensure_future(self.unmount(), loop=self._loop)

    def _stop_consumer(self):
        if self._consumer_fut is None or self._consumer_fut.done():
            return
        self._consumer_fut.cancel()

    def _start_consumer(self):
        if self._consumer_fut is not None:
            return

        self._consumer_fut = ensure_future(self._consume_all(), loop=self._loop)

    async def _unmount(self):
        if self._consumer_fut is not None:
            self._stop_consumer()
            self._consumer_fut.add_done_callback(lambda fut: fut.exception())

        await super(BaseIndependentConsumer, self)._unmount()


class BaseSink(BaseIndependentConsumer[Sink_co]):

    def consume_all(self):
        self._start_consumer()

    def pause_consumer(self) -> Future:
        self._stop_consumer()
        if self._consumer_fut is None:
            raise RuntimeError('Missing consumer future')
        return self._consumer_fut

    async def wait_until_eos(self):
        if self._consumer_fut is None:
            return
        try:
            await self._consumer_fut
        except StopAsyncIteration:
            return


class BaseSource(BaseProducer[Source_co], ABC):

    def __init__(self, *args, bus: 'Bus' = None, **kwargs):
        super(BaseSource, self).__init__(*args, **kwargs)

        self._bus = bus or Bus()

    @property
    def bus(self) -> 'Bus':
        return self._bus

    async def _unmount(self):
        self.bus.end_of_bus()

        await super(BaseSource, self)._unmount()


class BaseStage(BaseConsumer[Sink_co], BaseProducer[Source_co], ABC):

    async def _next_frame(self) -> Source_co:
        frame = await self.consume_frame()

        return await self.process_frame(frame)

    @abstractmethod
    async def process_frame(self, frame: Sink_co) -> Source_co:
        raise NotImplementedError()


class BaseIndependentConsumerStage(BaseIndependentConsumer[Source_co], BaseProducer[Source_co], ABC):
    async def _consume_frame(self) -> Source_co:
        while True:
            try:
                if self._iter is None:
                    raise RuntimeError('Iterator not initiated')
                frame = await self._iter.__anext__()
                await self.push_frame(frame)
                return frame
            except CancelledError:
                raise
            except StopAsyncIteration as ex:
                await self.push_frame(ex)
                raise
            except BaseException as ex:
                await self.push_frame(ex)

    def start_consumer(self):
        self._start_consumer()

    def pause_consumer(self) -> Future:
        self._stop_consumer()
        if self._consumer_fut is None:
            raise RuntimeError('Missing consumer future')
        return self._consumer_fut

    async def wait_until_finish_consumer(self):
        if self._consumer_fut is None:
            return
        await self._consumer_fut

    @abstractmethod
    async def push_frame(self, frame: Union[Source_co, BaseException]):
        raise NotImplementedError()

    async def _mount(self):
        await super(BaseIndependentConsumerStage, self)._mount()
        self.start_consumer()


class MessageFilter:
    def allow(self, message: 'Message') -> bool:
        return True


class SimpleMessageFilter(MessageFilter):

    def __init__(self,
                 msg_types: Iterable[str] = None,
                 msg_senders: Iterable[str] = None):
        self.msg_types = msg_types
        self.msg_senders = msg_senders

    def allow(self, message: 'Message') -> bool:
        if self.msg_types and message.type not in self.msg_types:
            return False
        if self.msg_senders and message.sender not in self.msg_senders:
            return False
        return True


class Bus:

    def __init__(self, loop: 'AbstractEventLoop' = None):
        self._loop = loop or get_event_loop()
        self._handlers: List[Tuple[MessageFilter, MessageHandler]] = []
        self._producers: WeakSet['BusMessageSource'] = WeakSet()
        self._consumers: Set['BusMessageSink'] = set()

    def send_message(self, message: 'Message'):

        for msg_filter, handler in self._handlers:
            if msg_filter.allow(message):
                ensure_future(handler(message), loop=self._loop)

        for producer in self._producers:
            ensure_future(producer.push_message(message), loop=self._loop)

    def add_handler(
            self,
            func: MessageHandler = None,
            *,
            msg_filter: MessageFilter = None,
            **kwargs
    ) -> Union[MessageHandler, Callable[[MessageHandler], MessageHandler]]:
        if msg_filter is None:
            msg_filter = SimpleMessageFilter(**kwargs)
        else:
            if len(kwargs):
                raise RuntimeError('Invalid extra argument when already defined a checker')

        def inner(f: MessageHandler) -> MessageHandler:
            self._handlers.append((msg_filter, f))
            return f

        if func:
            return inner(func)
        return inner

    def build_source(self) -> 'BusMessageSource':
        from .bus import BusMessageSource

        result = BusMessageSource()

        self._producers.add(result)

        return result

    def _get_bus_consumer(self, bus: 'Bus') -> 'BusMessageSink':
        for consumer in self._consumers:
            if consumer.bus == bus:
                return consumer

        raise ValueError('No consumer for bus')

    def pipe(self, bus: 'Bus'):
        from .bus import BusMessageSink

        if bus == self:
            return

        try:
            self._get_bus_consumer(bus)
        except ValueError:
            pass
        else:
            return

        sink = cast('BusMessageSink', bus.build_source() >> BusMessageSink(src_bus=bus, dst_bus=self))
        self._consumers.add(sink)

        sink.consume_all()

    def unpipe(self, bus: 'Bus'):
        try:
            consumer = self._get_bus_consumer(bus)
        except ValueError:
            pass
        else:
            ensure_future(consumer.unmount(), loop=self._loop)
            self._consumers.remove(consumer)

    def end_of_bus(self):
        [ensure_future(producer.end_of_bus(), loop=self._loop) for producer in self._producers]
        [self.unpipe(consumer.src_bus) for consumer in self._consumers]

    def __del__(self):
        try:
            self.end_of_bus()
        except RuntimeError:
            pass

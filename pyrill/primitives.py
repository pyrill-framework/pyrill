from abc import ABC
from asyncio import InvalidStateError, ensure_future, gather
from asyncio.futures import Future
from asyncio.locks import Lock
from asyncio.tasks import wait
from inspect import isawaitable
from typing import (Any, AsyncIterable, AsyncIterator, Awaitable, Callable,
                    Dict, List, Optional, Set, Tuple, Union, cast)
from weakref import ref

from .base import (BUS_MSG_ELEMENT_ERROR, BUS_MSG_ELEMENT_NULL,
                   BUS_MSG_ELEMENT_READY, BaseConsumer, BaseElement,
                   BaseProducer, BaseSink, BaseSource, BaseStage, Bus,
                   FrameSkippedError, Message, Sink_co, Source_co)
from .sources import AsyncSource

__all__ = ['Noop', 'Sequential', 'SkipIf', 'Branch', 'Tee', 'Aggregator', 'PrefixStream', 'SuffixStream',
           'Cache', 'CombineStreams', 'JoinFrame', 'JoinStreams']


class Noop(BaseStage[Source_co, Source_co]):
    async def process_frame(self, frame: Source_co) -> Source_co:
        return frame


class Sequential(Noop[Source_co]):

    def __init__(self, *args, **kwargs):
        super(Sequential, self).__init__(*args, **kwargs)

        self._lock = Lock()

    async def _next_frame(self) -> Source_co:
        async with self._lock:
            return await super(Sequential, self)._next_frame()


class SkipIf(BaseStage[Sink_co, Sink_co]):
    def __init__(self, *args, check_func: Callable[[Sink_co], bool], **kwargs):
        super(SkipIf, self).__init__(*args, **kwargs)

        self._check_func = check_func

    async def process_frame(self, frame: Sink_co) -> Sink_co:
        if self._check_func(frame):
            return frame

        raise FrameSkippedError()


class _InnerBranchProducer(BaseProducer[Source_co]):

    def __init__(self,
                 *args,
                 key: str,
                 parent: 'Branch',
                 **kwargs):
        super(_InnerBranchProducer, self).__init__(*args, **kwargs)

        self._key = key
        self._parent = ref(parent)

    @property
    def bus(self) -> 'Bus':
        return self._parent().bus

    async def _next_frame(self) -> Source_co:
        return await self._parent().request_frame(self._key)


class Branch(BaseConsumer[Sink_co]):
    def __init__(self,
                 *args,
                 branch_func: Callable[[Sink_co], Union[Any, Awaitable[Any]]],
                 on_unknown_branch: Callable[[Any, Sink_co], Union[Any, Awaitable[Any]]] = None,
                 **kwargs):
        super(Branch, self).__init__(*args, **kwargs)

        self._branch_func = branch_func
        self._branches: Dict[Any, _InnerBranchProducer[Sink_co]] = {}
        self._futs: 'Dict[Any, Future[Sink_co]]' = {}
        self._lock: Lock = Lock()
        self._on_unknown_branch: Optional[Callable[[Any, Sink_co], Union[Any, Awaitable[Any]]]] = on_unknown_branch

    def _remove_fut(self, fut: Future):
        self._futs = {k: v for k, v in self._futs.items() if v != fut}

    async def request_frame(self, key: Any) -> Sink_co:
        await self.mount()

        if key not in self._branches.keys():
            raise StopAsyncIteration()

        async with self._lock:
            try:
                fut = self._futs[key]
                if fut.done():
                    raise KeyError
            except KeyError:
                fut = self._futs[key] = Future()
                fut.add_done_callback(lambda fut: self._remove_fut(fut))

        if len(set(self._branches.keys()) & set(self._futs.keys())) == len(self._branches):
            await self.consume_frame()
        return await fut

    def add_branch(self, key: Any, consumer: BaseConsumer[Sink_co]) -> BaseConsumer[Sink_co]:
        inner = _InnerBranchProducer[Sink_co](key=key, parent=self, loop=self._loop)
        inner >> consumer
        self._branches[key] = inner

        return consumer

    def get_branch(self, key: Any) -> _InnerBranchProducer[Sink_co]:
        return self._branches[key]

    def remove_branch(self, key: Any):
        try:
            del self._branches[key]
        except KeyError:
            pass

        try:
            self._futs[key].set_exception(StopAsyncIteration())
            ensure_future(gather(self._futs[key], return_exceptions=True), loop=self._loop)
            del self._futs[key]
        except (KeyError, InvalidStateError):
            pass

    async def consume_frame(self) -> Sink_co:
        while True:
            try:
                frame = await super(Branch, self).consume_frame()
            except StopAsyncIteration:
                [self.remove_branch(key) for key in list(self._branches.keys())]
                raise
            except BaseException as ex:
                frame = ex

            try:
                branch: Union[Awaitable[str], str] = self._branch_func(frame)
                if isawaitable(branch):
                    branch = await cast(Awaitable[Any], branch)
                else:
                    branch = cast(Any, branch)
            except Exception:
                # TODO Process exception
                continue

            try:
                fut: Future = self._futs.pop(branch)
            except KeyError:
                if await self._unknown_branch(branch, frame):
                    return frame
                continue

            fut.set_result(frame)
            return frame

    async def _unknown_branch(self, branch: Any, frame: Sink_co):
        if self._on_unknown_branch:
            try:
                result = self._on_unknown_branch(branch, frame)
                if isawaitable(result):
                    await result
            except Exception:
                return False
            else:
                return True
        return False

    def on_unknown_branch(
            self,
            func: Callable[[Any, Sink_co], Union[Any, Awaitable[Any]]]
    ) -> Callable[[Any, Sink_co], Union[Any, Awaitable[Any]]]:
        self._on_unknown_branch = func
        return func


class _InnerTeeProducer(BaseProducer[Source_co]):

    def __init__(self, *args, parent: 'Tee', **kwargs):
        super(_InnerTeeProducer, self).__init__(*args, **kwargs)

        self._parent = ref(parent)

    @property
    def bus(self) -> 'Bus':
        return self._parent().bus

    async def _next_frame(self) -> Source_co:
        return await self._parent().request_frame(self)


class Tee(BaseConsumer[Source_co]):
    def __init__(self, *args, **kwargs):
        super(Tee, self).__init__(*args, **kwargs)

        self._consumers: 'Set[_InnerTeeProducer[Source_co]]' = set()
        self._futs: 'Dict[_InnerTeeProducer[Source_co], Future[Source_co]]' = {}
        self._lock: Lock = Lock()

    def _remove_fut(self, fut: Future):
        self._futs = {k: v for k, v in self._futs.items() if v != fut}

    async def request_frame(self, producer: '_InnerTeeProducer') -> Source_co:
        await self.mount()

        if producer not in self._consumers:
            raise StopAsyncIteration()

        async with self._lock:
            try:
                fut = self._futs[producer]
            except KeyError:
                fut = self._futs[producer] = Future()
                fut.add_done_callback(lambda fut: self._remove_fut(fut))

        if len(set(self._futs.keys()) & self._consumers) == len(self._consumers):
            await self.consume_frame()
        return await fut

    def add_consumer(self, consumer: 'BaseConsumer[Source_co]') -> 'BaseConsumer[Source_co]':
        inner = _InnerTeeProducer[Source_co](parent=self)
        inner >> consumer
        self._consumers.add(inner)
        return consumer

    def get_consumers(self) -> Set[_InnerTeeProducer[Source_co]]:
        return self._consumers.copy()

    def remove_consumer(self, consumer: 'BaseConsumer[Source_co]'):
        inner_producer: _InnerTeeProducer[Source_co] = cast(_InnerTeeProducer[Source_co], consumer.source)
        self._remove_inner_producer(inner_producer)

    def _remove_inner_producer(self, inner_producer: '_InnerTeeProducer[Source_co]'):
        try:
            self._consumers.remove(inner_producer)
        except KeyError:
            pass

        try:
            self._futs[inner_producer].set_exception(StopAsyncIteration())
            ensure_future(gather(self._futs[inner_producer], return_exceptions=True), loop=self._loop)
            del self._futs[inner_producer]
        except (KeyError, InvalidStateError):
            pass

    def consumer_count(self):
        return len(self._consumers)

    async def consume_frame(self) -> Source_co:
        try:
            frame = await super(Tee, self).consume_frame()
        except StopAsyncIteration:
            [self._remove_inner_producer(c) for c in self._consumers.copy()]
            raise

        for fut in self._futs.values():
            fut.set_result(frame)

        self._futs = {}

        return frame

    async def _unmount(self):
        [f.cancel() for f in self._futs.values() if not f.done()]
        await gather(*self._futs.values(), return_exceptions=True)

        await super(Tee, self)._unmount()

    def __rshift__(self, other: 'BaseElement') -> 'BaseElement':
        if isinstance(other, BaseConsumer):
            return self.add_consumer(other)
        else:
            return super(Tee, self).__rshift__(other)

    def __rlshift__(self, other: 'BaseElement') -> 'BaseElement':
        return self.__rshift__(other)


class Aggregator(BaseProducer[Tuple[Source_co]]):
    def __init__(self, *args, bus: 'Bus' = None, **kwargs):
        super(Aggregator, self).__init__(*args, **kwargs)

        self._sources: List[BaseProducer[Any]] = []
        self._bus = bus or Bus()

    @property
    def bus(self) -> 'Bus':
        return self._bus

    def add_source(self, source: 'BaseProducer[Any]'):
        self._sources.append(source)
        if source.bus is None:
            raise RuntimeError('Aggregator source must have a bus')
        self.bus.pipe(source.bus)

    def get_source(self, idx: int) -> 'BaseProducer[Any]':
        return self._sources[idx]

    def remove_source(self, source: 'BaseProducer[Any]'):
        try:
            self._sources.remove(source)
            self.bus.unpipe(source.bus)
        except IndexError:
            pass

    def source_count(self):
        return len(self._sources)

    async def _set_error(self, ex: BaseException):
        await super(Aggregator, self)._set_error(ex)
        [await src.unmount() for src in self._sources]

    async def _next_frame(self) -> 'Tuple[Any, ...]':
        done, _ = await wait([source.__anext__() for source in self._sources])
        return tuple(fut.result() for fut in done)

    def __lshift__(self, other: 'BaseElement') -> 'BaseElement':
        if isinstance(other, BaseProducer):
            self.add_source(other)
            return self
        else:
            return super(Aggregator, self).__lshift__(other)

    def __lrshift__(self, other: BaseElement) -> 'BaseElement':
        return self.__lshift__(other)


class PrefixStream(Noop[Source_co]):

    def __init__(self, *args, prefix: Source_co, **kwargs):
        super(PrefixStream, self).__init__(*args, **kwargs)

        self.prefix = prefix
        self._need_action = True

    async def _mount(self):
        self._need_action = True
        await super(PrefixStream, self)._mount()

    async def _consume_frame(self) -> Source_co:
        if self._need_action:
            self._need_action = False
            return self.prefix
        return await super(PrefixStream, self)._consume_frame()


class SuffixStream(Noop[Source_co]):

    def __init__(self, *args, suffix: Source_co, **kwargs):
        super(SuffixStream, self).__init__(*args, **kwargs)

        self.suffix = suffix
        self._need_action = True

    async def _mount(self):
        self._need_action = True
        await super(SuffixStream, self)._mount()

    async def _consume_frame(self) -> Source_co:
        if not self._need_action:
            raise StopAsyncIteration()
        try:
            return await super(SuffixStream, self)._consume_frame()
        except StopAsyncIteration:
            if self._need_action:
                self._need_action = False
                return self.suffix
            raise


class JoinFrame(Noop[Source_co]):
    EMPTY = object()

    def __init__(self, *args, sep: Source_co, **kwargs):
        super(JoinFrame, self).__init__(*args, **kwargs)

        self.sep = sep
        self._next_value: Union[Source_co, object] = self.EMPTY
        self._first = True

    async def _mount(self):
        self._first = True
        self._next_value: Source_co = self.EMPTY
        await super(JoinFrame, self)._mount()

    async def _unmount(self):
        self._next_value: Source_co = self.EMPTY

        await super(JoinFrame, self)._unmount()

    async def _consume_frame(self) -> Union[Source_co, object]:
        if self._first:
            self._first = False
            return await super(JoinFrame, self)._consume_frame()

        if self._next_value is not self.EMPTY:
            result = self._next_value
            self._next_value = self.EMPTY
            return result

        self._next_value = await super(JoinFrame, self)._consume_frame()
        return self.sep


class JoinStreams(BaseStage[BaseProducer[Source_co], Source_co]):
    _current_producer: 'Optional[BaseProducer[Source_co]]' = None
    _frame_iter: 'Optional[AsyncIterator[Source_co]]' = None

    async def _mount(self):
        self._frame_iter = None
        self._current_producer = None

        await super(JoinStreams, self)._mount()

    async def _unmount(self):
        self._frame_iter = None

        if self._current_producer is not None:
            await self._current_producer.unmount()

        self._current_producer = None

        await super(JoinStreams, self)._unmount()

    async def _consume_frame(self) -> Source_co:
        while True:
            if self._frame_iter is None:
                self._current_producer = await super(JoinStreams, self)._consume_frame()
                self._current_producer.bus.pipe(self.bus)
                self._frame_iter = self._current_producer.__aiter__()
            try:
                return await self._frame_iter.__anext__()
            except StopAsyncIteration:
                self._current_producer.bus.unpipe(self.bus)
                self._frame_iter = None
                await self._current_producer.unmount()
                self._current_producer = None
                continue

    async def process_frame(self, frame: 'BaseProducer[Source_co]') -> 'BaseProducer[Source_co]':
        return frame


class CombineStreams(BaseProducer[Tuple[Source_co]]):
    def __init__(self, *args, bus: 'Bus' = None, **kwargs):
        super(CombineStreams, self).__init__(*args, **kwargs)

        self._sources: Dict[BaseProducer[Source_co], Optional[Future]] = {}
        self._bus = bus or Bus()

    @property
    def bus(self) -> 'Bus':
        return self._bus

    def add_source(self, source: 'BaseProducer[Source_co]') -> 'CombineStreams':
        self._sources[source] = None
        if source.bus is None:
            raise RuntimeError('CombineStream source must have a bus')
        self.bus.pipe(source.bus)

        return self

    def remove_source(self, source: BaseProducer[Source_co]):
        try:
            if self._sources[source] is not None and not self._sources[source].done():
                self._sources[source].set_exception(StopAsyncIteration())
            del self._sources[source]
            self.bus.unpipe(source.bus)
            ensure_future(source.unmount(), loop=self._loop)
        except IndexError:
            pass

    def source_count(self):
        return len(self._sources)

    async def _set_error(self, ex: BaseException):
        await super(CombineStreams, self)._set_error(ex)
        [await src.unmount() for src in self._sources]

    async def _next_frame(self) -> 'Tuple[Any, ...]':
        done, _ = await wait([source.__anext__() for source in self._sources])
        return tuple(fut.result() for fut in done)

    def __lshift__(self, other: 'BaseElement') -> 'BaseElement':
        if isinstance(other, BaseProducer):
            self.add_source(other)
            return self
        else:
            return super(CombineStreams, self).__lshift__(other)

    def __lrshift__(self, other: 'BaseElement') -> 'BaseElement':
        return self.__lshift__(other)


class BinMixin:

    def __init__(self, *args, **kwargs):
        super(BinMixin, self).__init__(*args, **kwargs)

        self._inner_bus = Bus()

    def inner_bus(self) -> 'Bus':
        return self._inner_bus

    async def _msg_forward(self, msg: 'Message'):
        if msg.type == [BUS_MSG_ELEMENT_READY, BUS_MSG_ELEMENT_NULL]:
            return
        if msg.type == BUS_MSG_ELEMENT_ERROR:
            await self._set_error(msg.params['ex'])
            return
        msg.sender = self.name
        self._send_message(msg)


class BaseBinProducer(BinMixin, BaseProducer[Source_co], ABC):

    def __init__(self, *args, **kwargs):
        super(BaseBinProducer, self).__init__(*args, **kwargs)

        self._downstream_producer: Optional[BaseProducer[Source_co]] = None
        self._inner_iter: Optional[AsyncIterable[Source_co]] = None

    def set_downstream_producer(self, elem: 'BaseProducer'):
        if self._downstream_producer == elem:
            return
        if elem.bus is None:
            raise RuntimeError('Downstream bin elements must have a bus')

        self._downstream_producer = elem

        if self._inner_bus == elem.bus:
            return

        self._inner_bus = elem.bus

        elem.bus.add_handler(self._msg_forward)

    async def _mount(self):
        await self._downstream_producer.mount()
        self._inner_iter = self._downstream_producer.__aiter__()

        await super(BaseBinProducer, self)._mount()

    async def _unmount(self):
        await self._downstream_producer.unmount()
        self._inner_iter = None

        await super(BaseBinProducer, self)._unmount()

    async def __anext__(self) -> Source_co:
        return await self._downstream_producer.__anext__()

    async def _next_frame(self) -> Source_co:
        pass


class BaseBinConsumer(BinMixin, BaseConsumer[Sink_co], ABC):

    def __init__(self, *args, **kwargs):

        super(BaseBinConsumer, self).__init__(*args, **kwargs)

        self._upstream_bridge = AsyncSource(bus=self._inner_bus, source=self.source)

        self._inner_bus.add_handler(self._msg_forward)

        self._upstream_consumer: 'Optional[BaseConsumer[Sink_co]]' = None
        self._iter: 'Optional[AsyncIterable[Sink_co]]' = None

    def set_source(self, value: 'Optional[BaseProducer[Sink_co]]' = None):
        super(BaseBinConsumer, self).set_source(value)
        try:
            self._upstream_bridge.source = value
        except AttributeError:
            pass

    def set_upstream_consumer(self, elem: 'BaseConsumer'):
        if self._upstream_consumer == elem:
            return

        self._upstream_consumer = elem
        self._upstream_bridge >> elem

    async def _mount(self):
        await self._upstream_consumer.mount()
        await super(BaseConsumer, self)._mount()

    async def _unmount(self):
        await self._upstream_consumer.unmount()
        await super(BaseConsumer, self)._unmount()


class BaseBinStage(BaseBinConsumer[Sink_co], BaseBinProducer[Source_co], ABC):
    pass


class BaseBinSource(BaseBinProducer[Source_co], BaseSource[Source_co], ABC):
    pass


class BaseBinSink(BaseBinConsumer[Sink_co], BaseSink[Sink_co], ABC):
    pass


class CacheBridge(BaseStage[Source_co, Source_co]):
    _idx: int = 0

    async def _mount(self):
        self._idx = 0

        await super(CacheBridge, self)._mount()

    async def _consume_frame(self) -> Source_co:
        try:
            frame = await cast(Cache, self.source).get_cached_item(self._idx)
        except StopAsyncIteration:
            raise
        except BaseException:
            self._idx += 1
            raise
        else:
            self._idx += 1
            return frame

    async def process_frame(self, frame: Source_co) -> Source_co:  # pragma: no cover
        return frame


class Cache(BaseStage[Source_co, Source_co]):
    _cached_data: 'Optional[List[Source_co]]' = None
    _next_fut: 'Optional[Future]' = None

    async def _mount(self):
        if self._cached_data is None:
            self._cached_data = []

        await super(Cache, self)._mount()

    async def get_cached_item(self, idx: int) -> Source_co:
        while True:
            try:
                frame = self._cached_data[idx]
            except IndexError:
                if len(self._cached_data) and isinstance(self._cached_data[-1], StopAsyncIteration):
                    raise self._cached_data[-1]

                await self.mount()

                if self._next_fut is None:
                    self._next_fut = Future()
                    try:
                        await self.__anext__()
                    except BaseException:
                        pass
                    self._next_fut.set_result(None)
                    self._next_fut = None
                else:
                    await self._next_fut

            else:
                if isinstance(frame, BaseException):
                    raise frame
                return frame

    def __aiter__(self):
        return CacheBridge(source=self).__aiter__()

    async def _next_frame(self) -> Source_co:
        try:
            return await super(Cache, self)._next_frame()
        except BaseException as ex:
            self._cached_data.append(ex)
            raise

    async def process_frame(self, frame: Source_co) -> Source_co:
        self._cached_data.append(frame)
        return frame

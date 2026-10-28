from abc import ABC, abstractmethod
from asyncio import (
    AbstractEventLoop,
    CancelledError,
    Event,
    Future,
    Lock,
    Task,
    create_task,
    current_task,
    get_running_loop,
    wait,
)
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import suppress
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, field
from enum import StrEnum
from logging import Logger, LoggerAdapter, getLogger
from typing import Any, Protocol, Self, TypeVar, cast, get_args
from uuid import uuid4
from weakref import ref

__all__ = [
    "MSG_ELEMENT_ERROR",
    "MSG_ELEMENT_NULL",
    "MSG_ELEMENT_READY",
    "MSG_FRAME_SKIPPED",
    "MSG_STREAM_START",
    "MSG_STREAM_STOP",
    "BaseConsumer",
    "BaseDecoupledStage",
    "BaseElement",
    "BaseExternalSource",
    "BaseFullSink",
    "BaseParallelProducer",
    "BaseProducer",
    "BaseSink",
    "BaseSource",
    "BaseStage",
    "ElementState",
    "FrameSkipped",
    "Message",
    "MessageCallback",
    "Pipeline",
    "active_pipeline",
]

MSG_STREAM_START = "stream-start"
MSG_STREAM_STOP = "stream-stop"
MSG_FRAME_SKIPPED = "frame-skipped"
MSG_ELEMENT_READY = "element-ready"
MSG_ELEMENT_NULL = "element-null"
MSG_ELEMENT_ERROR = "element-error"


type MessageCallback = Callable[
    [MessageBrokerMixin, Message], bool | tuple[bool, bool] | None
]


# Explicit variance: inferring it would depend on BaseProducer's own variance,
# a cycle that mypy resolves as invariant (rejecting e.g. Producer[str] >> Sized).
_T_contra = TypeVar("_T_contra", contravariant=True)
_R_co = TypeVar("_R_co", covariant=True)


class _ConsumerOf(Protocol[_T_contra, _R_co]):  # pyright: ignore[reportInvalidTypeVarUse]
    def __rrshift__(self, other: BaseProducer[_T_contra], /) -> _R_co: ...


class FrameSkipped(BaseException):
    """
    Signal raised inside an element to drop the current frame without ending the stream.

    Raise it from ``_next_frame`` or ``_process_frame``. The element catches it, sends a
    ``frame-skipped`` message on the bus and moves on to the next frame, so downstream
    elements never see the skipped one. It derives from :class:`BaseException`, so a
    generic ``except Exception`` in user code does not swallow it.

    :param frame: Optional frame (or reason) attached to the signal and to the message.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseStage, FrameSkipped
        from pyrill.sources import IterableSource


        class OnlyEven(BaseStage[int, int]):
            async def _process_frame(self, frame: int) -> int:
                if frame % 2:
                    raise FrameSkipped(frame)
                return frame


        stage = IterableSource(source=[1, 2, 3, 4]) >> OnlyEven()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [2, 4]
    """

    def __init__(self, frame: Any = None):
        self.frame = frame


@dataclass
class Message:
    """
    Event published by an element on its pipeline bus.

    Elements send messages for lifecycle changes, stream activity and skipped frames.
    The ``type`` can be a ``MSG_*`` constant of this module (``"element-ready"``,
    ``"element-null"``, ``"element-error"``, ``"frame-skipped"``, ...).

    :param sender: Name of the element that sent the message.
    :param pipeline: Name of the originating pipeline.
    :param type: Message type, for example ``"element-ready"``.
    :param params: Extra data, for example ``{"frame": 3}`` or ``{"ex": error}``.

    Usage:

    .. code-block:: python

        from pyrill.base import Message
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1])
        received: list[Message] = []
        source.add_message_callback(lambda element, message: received.append(message))

        await source.mount()
        types = [message.type for message in received]
        # ["element-ready"]
    """

    sender: str
    pipeline: str
    type: str
    params: dict[str, Any] = field(default_factory=dict[str, Any])


class ElementState(StrEnum):
    """
    Lifecycle state of an element.

    An element starts in ``NULL``. :meth:`BaseElement.mount` moves it through
    ``MOUNTING`` to ``READY``, and :meth:`BaseElement.unmount` moves it back through
    ``UNMOUNTING`` to ``NULL``. An unhandled failure leaves it in ``ERROR``, from which
    it cannot be mounted again. Members are strings, so ``state == "ready"`` is valid.

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2])
        before = source.state

        await source.mount()
        during = source.state
        await source.unmount()

        states = [before, during, source.state]
        # ["null", "ready", "null"]
    """

    NULL = "null"
    MOUNTING = "mounting"
    READY = "ready"
    ERROR = "error"
    UNMOUNTING = "unmounting"


class MessageBrokerMixin:
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ):
        self._message_callbacks: list[tuple[MessageCallback, str | None]] = []

        super().__init__(*args, **kwargs)

    def _send_message_inner(self, message: Message) -> bool:
        to_remove: list[tuple[MessageCallback, str | None]] = []
        propagate = True
        for callback, message_type in self._message_callbacks:
            if message_type is None or message_type == message.type:
                result = callback(self, message)
                if result is None:
                    continue
                if not isinstance(result, tuple):
                    result = (result, True)
                remove, propagate = result
                if remove:
                    to_remove.append((callback, message_type))
                if not propagate:
                    break

        for item in to_remove:
            self._message_callbacks.remove(item)

        return propagate

    def _send_message(self, message: Message) -> None:
        self._send_message_inner(message)

    def add_message_callback(
        self,
        callback: MessageCallback,
        message_type: str | None = None,
    ) -> None:
        """
        Register a callback for messages on this element or pipeline.

        The callback is called as ``callback(broker, message)`` for every message, or only
        for those whose ``type`` equals ``message_type``. It may return ``None`` (keep it and
        propagate), a bool ``remove`` or a tuple ``(remove, propagate)``; ``remove=True``
        unregisters the callback and ``propagate=False`` stops the message from reaching the
        pipeline and the remaining callbacks.
        """
        self._message_callbacks.append((callback, message_type))


class Pipeline(MessageBrokerMixin):
    """
    Container that groups elements and gives them a logger, an event loop and a bus.

    Every element belongs to exactly one pipeline. When none is given, the element uses
    the one stored in the ``active_pipeline`` context variable (a pipeline named
    ``"default"`` unless changed). Elements can only be connected to each other when they
    share the same pipeline. The pipeline keeps weak references, so it never keeps an
    element alive.

    :param name: Pipeline name, used in log records. Default: a random UUID.
    :param logger: Base logger. Default: the logger named after the pipeline.
    :param loop: Event loop to expose through :attr:`loop`. Default: the running loop.

    Usage:

    .. code-block:: python

        from pyrill.base import Pipeline
        from pyrill.sinks import Last
        from pyrill.sources import IterableSource

        pipeline = Pipeline("demo")
        source = IterableSource(source=[1, 2, 3], pipeline=pipeline)
        sink = source >> Last(pipeline=pipeline)

        last = await sink.get_frame()
        # 3
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        logger: Logger | None = None,
        loop: AbstractEventLoop | None = None,
    ):
        super().__init__()

        self.name = name or str(uuid4())
        self._logger = LoggerAdapter(
            logger or getLogger(name or __name__), {"pipeline": self.name}
        )
        self._loop = loop
        self.elements: dict[int, ref[BaseElement]] = {}
        self._orphan_tasks: set[Task[Any]] = set()

    @property
    def logger(self) -> Logger:
        """
        Logger of the pipeline; records carry the pipeline name as extra data.
        """
        return cast(Logger, self._logger)

    @property
    def loop(self) -> AbstractEventLoop:
        """
        Event loop given at creation, or the currently running loop.
        """
        return self._loop or get_running_loop()

    def include_element(self, element: BaseElement) -> None:
        """
        Register an element with a weak reference. Elements call this on creation.
        """
        key = id(element)
        self.elements[key] = ref(element, lambda _: self.elements.pop(key, None))

    def remove_element(self, element: BaseElement) -> None:
        """
        Unregister an element. It is not an error if it was not registered.
        """
        self.elements.pop(id(element), None)

    def send_message(self, message: Message) -> None:
        """
        Log a message at debug level and dispatch it to pipeline callbacks.
        """
        self._logger.debug("Sending message: %s", message)
        self._send_message(message)

    def create_orphan_task(self, coro: Coroutine[Any, Any, Any]) -> Task[Any]:
        """
        Start a background task and keep a strong reference until it finishes.
        """
        task = create_task(coro)
        self._orphan_tasks.add(task)
        task.add_done_callback(lambda t: self._orphan_tasks.discard(t))
        return task


active_pipeline: ContextVar[Pipeline] = ContextVar(
    "active_pipeline",
    default=Pipeline("default"),  # noqa: B039
)


class BaseElement(MessageBrokerMixin, ABC):
    """
    Base class of every element: name, pipeline, lifecycle state and message callbacks.

    It provides the lifecycle that all elements share. :meth:`mount` runs ``_mount`` and
    moves the element from ``NULL`` to ``READY``; :meth:`unmount` runs ``_unmount`` and
    moves it back to ``NULL``. Both are idempotent and protected by a lock. If ``_mount``
    or ``_unmount`` raises, the element runs its cleanup, enters ``ERROR`` (see
    :class:`ElementState`) and re-raises. Mounting an element in ``ERROR`` raises
    :class:`RuntimeError`.

    Subclasses override ``_mount`` and ``_unmount`` to acquire and release resources.
    Always call ``await super()._mount()`` and ``await super()._unmount()``. Most users
    extend :class:`BaseSource`, :class:`BaseStage` or :class:`BaseSink` instead of this
    class.

    :param name: Element name. Default: class name plus a random UUID.
    :param pipeline: Owning :class:`Pipeline`. Default: the active pipeline.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseElement


        class Resource(BaseElement):
            opened = False

            async def _mount(self) -> None:
                self.opened = True
                await super()._mount()

            async def _unmount(self) -> None:
                self.opened = False
                await super()._unmount()


        element = Resource("res")

        await element.mount()
        opened = element.opened
        # True
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        pipeline: Pipeline | None = None,
    ):
        super().__init__()

        self._pipeline = pipeline or active_pipeline.get()

        self.name = name or self.new_name()
        self._logger = LoggerAdapter(self.pipeline.logger, {"element": self.name})

        self._state = ElementState.NULL
        self._state_lock = Lock()
        self._last_error: BaseException | None = None

        self.pipeline.include_element(self)

    @property
    def state(self) -> ElementState:
        """
        Current :class:`ElementState` of the element.
        """
        return self._state

    @property
    def pipeline(self) -> Pipeline:
        """
        Pipeline that owns the element.
        """
        if self._pipeline:
            return self._pipeline
        raise RuntimeError("Pipeline reference is no longer valid")

    @classmethod
    def new_name(cls) -> str:
        """
        Build a unique default name from the class name and a UUID.
        """
        return "-".join([cls.__name__, str(uuid4())])

    def _build_message(self, type: str, **params: Any) -> Message:
        return Message(
            sender=self.name, pipeline=self.pipeline.name, type=type, params=params
        )

    def _send_message(self, message: Message) -> None:
        if not self._send_message_inner(message):
            return

        self.pipeline.send_message(message)

    async def _mount(self) -> None:
        """Mount the element. This method should be overridden by subclasses to implement custom mounting logic."""

    async def _unmount(self) -> None:
        """Unmount the element. This method should be overridden by subclasses to implement custom unmounting logic."""

    async def _set_error(self, ex: BaseException) -> None:
        async with self._state_lock:
            if self._state == ElementState.ERROR:
                return
            self._logger.debug("Setting error")
            try:
                with suppress(Exception):
                    await self._unmount()
            finally:
                self._state = ElementState.ERROR
                self._last_error = ex
                self._logger.debug("Error set")
                with suppress(Exception):
                    self._send_message(self._build_message(MSG_ELEMENT_ERROR, ex=ex))

    async def mount(self) -> None:
        """
        Move the element from ``NULL`` to ``READY``; do nothing if it is not in ``NULL``.
        """
        if self._state == ElementState.ERROR:
            raise RuntimeError("Element has an error")
        try:
            async with self._state_lock:
                if self._state != ElementState.NULL:
                    return
                self._logger.debug("Mounting")
                self._state = ElementState.MOUNTING
                await self._mount()
                self._state = ElementState.READY
                self._logger.debug("Ready")
                self._send_message(self._build_message(MSG_ELEMENT_READY))
        except Exception as ex:
            await self._set_error(ex)
            raise

    async def unmount(self) -> None:
        """
        Move the element from ``READY`` to ``NULL``; do nothing if it is not ``READY``.
        """
        try:
            async with self._state_lock:
                if self._state != ElementState.READY:
                    return
                self._logger.debug("Unmounting")
                self._state = ElementState.UNMOUNTING
                await self._unmount()
                self._state = ElementState.NULL
                self._logger.debug("Null state")
                self._send_message(self._build_message(MSG_ELEMENT_NULL))
        except BaseException as ex:
            await self._set_error(ex)
            raise


class BaseProducer[Source_co](AsyncIterator[Source_co], BaseElement, ABC):
    """
    Base class of every element that produces frames and can be read as an async iterator.

    ``async for`` (``__anext__``) requires the element to be mounted. It calls
    ``_next_frame`` in a loop and returns its result. A :class:`FrameSkipped` raised there
    drops the frame and tries again; ``StopAsyncIteration`` ends the stream and unmounts
    the element; any other exception puts the element in ``ERROR`` and is re-raised.
    Concurrent readers are serialised by a lock. Unmounting cancels a read in progress.

    A subclass implements ``async def _next_frame(self) -> T``. A producer is connected to
    a consumer with ``producer >> consumer``, which returns the consumer.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseProducer


        class Counter(BaseProducer[int]):
            def __init__(self, limit: int) -> None:
                super().__init__()
                self.limit = limit
                self.count = 0

            async def _next_frame(self) -> int:
                if self.count >= self.limit:
                    raise StopAsyncIteration()
                self.count += 1
                return self.count


        producer = Counter(3)

        await producer.mount()
        frames = [frame async for frame in producer]
        # [1, 2, 3]
    """

    _consumer_task: Task[Any] | None = None

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._consumer_lock = Lock()
        self._closed = False

    async def __anext__(self) -> Source_co:
        if self._closed:
            raise StopAsyncIteration()
        async with self._consumer_lock:
            try:
                self._consumer_task = current_task()
                await self._ensure_ready()

                try:
                    while True:
                        try:
                            return await self._next_frame_wrapper()
                        except FrameSkipped as ex:
                            self._logger.debug(
                                f"Skipped frame: {ex.frame!r}",
                                extra={"frame": ex.frame},
                            )
                            with suppress(Exception):
                                self._send_message(
                                    self._build_message(
                                        MSG_FRAME_SKIPPED, frame=ex.frame
                                    )
                                )

                except StopAsyncIteration:
                    self._closed = True
                    await self.unmount()
                    raise
                except Exception as ex:
                    self._closed = True
                    await self._set_error(ex)

                    raise
            finally:
                self._consumer_task = None

    async def _ensure_ready(self) -> None:
        if self._state is ElementState.READY:
            return
        raise RuntimeError("Element is not ready")

    async def _next_frame_wrapper(self) -> Source_co:
        return await self._next_frame()

    @abstractmethod
    async def _next_frame(self) -> Source_co:  # pragma: nocover
        raise NotImplementedError()

    async def _unmount(self) -> None:
        self._closed = True
        if (
            self._consumer_task is not None
            and self._consumer_task is not current_task()
            and not self._consumer_task.done()
        ):
            self._consumer_task.cancel("Pipeline element unmounted")

        await super()._unmount()

    async def _mount(self) -> None:
        self._closed = False
        await super()._mount()

    def get_product_type(self) -> type[Source_co]:
        """
        Return the type argument of a parametrised instance such as ``Source[int]()``.

        Raise :class:`TypeError` if the instance was created without a type argument.
        """
        generic_type = getattr(self, "__orig_class__", None)
        type_args = get_args(generic_type)
        if not type_args:
            raise TypeError("Product type is not available at runtime")
        return cast(type[Source_co], type_args[0])

    def __aiter__(self) -> AsyncIterator[Source_co]:  # noqa: PYI034
        return self

    def __rshift__[R](self, other: _ConsumerOf[Source_co, R]) -> R:
        # Delegate to any element that accepts producers (consumers, Zip, ...),
        # including one of the same type, for which Python skips __rrshift__.
        if not hasattr(type(other), "__rrshift__"):
            return NotImplemented
        return other.__rrshift__(self)


class BaseParallelProducer[Source_co](AsyncIterator[Source_co], BaseElement, ABC):
    """
    Base class of producers that serve several concurrent readers.

    Unlike :class:`BaseProducer`, each ``__anext__`` call registers a future and a single
    background task calls ``_next_frame`` and hands every produced frame to one waiting
    reader, without a lock between readers. The background task mounts the element on
    demand, sends ``frame-skipped`` messages for :class:`FrameSkipped`, and unmounts on
    ``StopAsyncIteration``. The task stops when no reader is waiting.

    A subclass implements ``async def _next_frame(self) -> T``.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseParallelProducer


        class Counter(BaseParallelProducer[int]):
            count = 0

            async def _next_frame(self) -> int:
                if self.count >= 3:
                    raise StopAsyncIteration()
                self.count += 1
                return self.count


        producer = Counter()

        frame = await anext(producer)
        # 1
    """

    _futs: set[Future[Source_co]]
    _running_fut: Future[None] | None
    _event: Event

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._futs = set()
        self._running_fut = None
        self._event = Event()

    async def __anext__(self) -> Source_co:
        fut = Future[Source_co]()
        self._futs.add(fut)
        self._event.set()
        fut.add_done_callback(lambda f: self._discard_fut(f))
        self._ensure_running()
        return await fut

    def _discard_fut(self, fut: Future[Source_co]) -> None:
        self._futs.discard(fut)
        if not self._futs and self._running_fut is not None:
            self._running_fut.cancel()

    def _ensure_running(self) -> None:
        if self._running_fut is not None:
            return

        self._running_fut = create_task(self._run(), context=copy_context())

        def running_done_callback(f: Future[None]) -> None:
            if self._running_fut is f:
                self._running_fut = None

        self._running_fut.add_done_callback(running_done_callback)

    async def _get_next_fut(self) -> Future[Source_co]:
        while not self._futs:
            self._event.clear()
            await self._event.wait()

        return self._futs.pop()

    async def _run(self) -> None:
        while self._futs:
            try:
                if self._state is not ElementState.READY:
                    await self.mount()

                frame = await self._next_frame()

                self._logger.debug(f"Produced frame: {frame}")
                fut = await self._get_next_fut()
                fut.set_result(frame)
            except StopAsyncIteration as ex:
                self._logger.debug("Stream finished")
                if self._futs:
                    while fut := self._futs.pop():
                        fut.set_exception(ex)
                await self.unmount()
                return
            except FrameSkipped as ex:
                self._send_message(
                    self._build_message(MSG_FRAME_SKIPPED, frame=ex.frame)
                )
                continue
            except Exception as ex:  # noqa: BLE001
                await self._set_error(ex)

    @abstractmethod
    async def _next_frame(self) -> Source_co:  # pragma: nocover
        raise NotImplementedError()

    async def _unmount(self) -> None:
        for task in self._futs:
            if not task.done() and task != current_task():
                task.cancel("Unmounted element")

        await super()._unmount()

    def __aiter__(self) -> Self:
        return self


class BaseConsumer[Sink_co](BaseElement):
    """
    Base class of every element that reads frames from an upstream producer.

    The upstream producer is set with the ``source`` argument, the :attr:`source`
    property, :meth:`connect` or the operators ``producer >> consumer`` and
    ``consumer << producer``. Mounting a consumer mounts its source and starts iterating
    it; unmounting a consumer unmounts the source. The source cannot be changed while the
    consumer is ``READY``.

    Subclasses call :meth:`consume_frame` to read the next upstream frame. It skips
    frames dropped with :class:`FrameSkipped`, lets ``StopAsyncIteration`` pass through
    and puts the element in ``ERROR`` for any other exception. Most users extend
    :class:`BaseSink` or :class:`BaseStage` instead.

    :param source: Upstream producer. Default: ``None`` (set it before mounting).

    Usage:

    .. code-block:: python

        from pyrill.base import BaseConsumer
        from pyrill.sources import IterableSource


        class Summer(BaseConsumer[int]):
            async def total(self) -> int:
                await self.mount()
                total = 0
                try:
                    while True:
                        total += await self.consume_frame()
                except StopAsyncIteration:
                    return total


        consumer = IterableSource(source=[1, 2, 3]) >> Summer()

        total = await consumer.total()
        # 6
    """

    _source: BaseProducer[Sink_co] | None = None
    _iter: AsyncIterator[Sink_co] | None = None

    def __init__(
        self, *args: Any, source: BaseProducer[Sink_co] | None = None, **kwargs: Any
    ):
        super().__init__(*args, **kwargs)

        self.source = source

    @property
    def source(self) -> BaseProducer[Sink_co] | None:
        """
        Upstream producer, or ``None`` when not connected.
        """
        return self._source

    @source.setter
    def source(self, value: BaseProducer[Sink_co] | None = None) -> None:
        self.set_source(value)

    def set_source(self, value: BaseProducer[Sink_co] | None = None) -> None:
        """
        Set the upstream producer. Raise :class:`RuntimeError` if the element is READY.
        """
        if self._source == value:
            return
        if self.state == ElementState.READY:
            raise RuntimeError("Unmount before change source")
        self._source = value

    async def _mount(self) -> None:
        if self._source is None:
            raise RuntimeError("Not source set")

        await self._source.mount()
        self._iter = aiter(self._source)
        await super()._mount()

    async def _unmount(self) -> None:
        # A shared source (e.g. Tee) hands each consumer its own iterator element;
        # unmount that one so the other consumers of the source keep running.
        upstream = self._iter if isinstance(self._iter, BaseElement) else self._source
        self._iter = None

        with suppress(Exception):
            if upstream is not None:
                await upstream.unmount()
        await super()._unmount()

    async def _consume_frame(self) -> Sink_co:
        if self._iter is None or self._state is not ElementState.READY:
            raise RuntimeError("Iterator has not been initiated or error state")

        return await anext(self._iter)

    async def consume_frame(self) -> Sink_co:
        """
        Read the next upstream frame, skipping :class:`FrameSkipped`.

        ``StopAsyncIteration`` passes through at the end of the stream; other exceptions
        put the element in ``ERROR`` and are re-raised.
        """
        while True:
            try:
                return await self._consume_frame()
            except FrameSkipped as ex:
                self._send_message(
                    self._build_message(MSG_FRAME_SKIPPED, frame=ex.frame)
                )
                continue
            except StopAsyncIteration:
                self._send_message(self._build_message(MSG_STREAM_STOP))
                raise
            except Exception as ex:
                await self._set_error(ex)
                raise

    def connect(self, other: BaseProducer[Sink_co]) -> None:
        """
        Use ``other`` as the upstream producer.

        Raise :class:`RuntimeError` if it belongs to a different pipeline.
        """
        if other.pipeline is not self.pipeline:
            raise RuntimeError("Cannot connect elements from different pipelines")
        self.source = other

    # Both ``consumer << producer`` and ``producer >> consumer`` return the
    # downstream element (the consumer).
    def __lshift__(self, other: BaseProducer[Sink_co]) -> Self:
        if not isinstance(other, BaseProducer):  # pyright: ignore[reportUnnecessaryIsInstance]
            return NotImplemented
        self.connect(other)
        return self

    # BaseProducer.__rshift__ delegates here, so both paths return the same type.
    def __rrshift__(self, other: BaseProducer[Sink_co]) -> Self:  # type: ignore[misc]
        if not isinstance(other, BaseProducer):  # pyright: ignore[reportUnnecessaryIsInstance]
            return NotImplemented
        self.connect(other)
        return self


class BaseSource[Source_co](BaseProducer[Source_co], ABC):
    """
    Base class of elements that originate frames (no upstream input).

    It adds nothing to :class:`BaseProducer`; it marks the start of a chain. A subclass
    implements ``_next_frame`` and, if it needs resources, ``_mount`` and ``_unmount``.
    Raise ``StopAsyncIteration`` from ``_next_frame`` to end the stream.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseSource


        class Letters(BaseSource[str]):
            def __init__(self, text: str) -> None:
                super().__init__()
                self._text = text

            async def _mount(self) -> None:
                self._iter = iter(self._text)
                await super()._mount()

            async def _next_frame(self) -> str:
                try:
                    return next(self._iter)
                except StopIteration:
                    raise StopAsyncIteration()


        source = Letters("abc")

        await source.mount()
        frames = [frame async for frame in source]
        # ["a", "b", "c"]
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._initiated = False

    async def _unmount(self) -> None:
        await super()._unmount()
        self._initiated = False

    async def _ensure_ready(self) -> None:
        await super()._ensure_ready()

        if self._initiated is True:
            return

        self._send_message(self._build_message(MSG_STREAM_START))
        self._initiated = True

    async def _next_frame_wrapper(self) -> Source_co:
        try:
            return await self._next_frame()
        except StopAsyncIteration:
            self._send_message(self._build_message(MSG_STREAM_STOP))
            raise


class BaseExternalSource[Source_co](BaseSource[Source_co], ABC):
    """
    Base class of sources fed from outside the pipeline.

    Application code calls :meth:`push_frame` to inject frames and :meth:`end_of_stream`
    to close the stream, while the pipeline reads them as usual. A subclass implements
    ``push_frame``, ``end_of_stream`` and ``_next_frame``, typically around an internal
    queue. :class:`pyrill.queues.QueueSource` is a ready-made implementation of the idea.

    Usage:

    .. code-block:: python

        from asyncio import Queue

        from pyrill.base import BaseExternalSource


        class Inbox(BaseExternalSource[str]):
            def __init__(self) -> None:
                super().__init__()
                self._queue: Queue[str | Exception | None] = Queue()

            async def push_frame(self, frame: str | Exception) -> None:
                await self._queue.put(frame)

            async def end_of_stream(self) -> None:
                await self._queue.put(None)

            async def _next_frame(self) -> str:
                frame = await self._queue.get()
                if frame is None:
                    raise StopAsyncIteration()
                if isinstance(frame, Exception):
                    raise frame
                return frame


        inbox = Inbox()
        await inbox.mount()
        await inbox.push_frame("a")
        await inbox.push_frame("b")
        await inbox.end_of_stream()

        frames = [frame async for frame in inbox]
        # ["a", "b"]
    """

    @abstractmethod
    async def push_frame(self, frame: Source_co | Exception) -> None:
        """
        Inject a frame into the source; an exception is raised to the reader instead.
        """
        raise NotImplementedError()

    @abstractmethod
    async def end_of_stream(self) -> None:
        """
        Close the stream after the frames already pushed.
        """
        raise NotImplementedError()


class BaseSink[Sink_co](BaseConsumer[Sink_co], ABC):
    """
    Base class of elements that end a chain: they consume frames and produce none.

    It adds nothing to :class:`BaseConsumer`. Subclass it directly when you want to
    control the reading loop with :meth:`BaseConsumer.consume_frame`; extend
    :class:`BaseFullSink` when you only need to handle each frame, or
    :class:`pyrill.sinks.BaseOneFrameSink` when the sink returns a single result.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseSink
        from pyrill.sources import IterableSource


        class Count(BaseSink[str]):
            async def run(self) -> int:
                await self.mount()
                count = 0
                try:
                    while True:
                        await self.consume_frame()
                        count += 1
                except StopAsyncIteration:
                    return count


        sink = IterableSource(source=["a", "b"]) >> Count()

        count = await sink.run()
        # 2
    """


class BaseFullSink[Sink_co](BaseSink[Sink_co], ABC):
    """
    Base class of sinks that process every frame until the stream ends.

    :meth:`consume_all` mounts the sink, calls ``_process_frame`` for each frame and
    unmounts at the end (also on error). A subclass only implements
    ``async def _process_frame(self, frame) -> None``.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseFullSink
        from pyrill.sources import IterableSource


        class Collect(BaseFullSink[int]):
            def __init__(self) -> None:
                super().__init__()
                self.items: list[int] = []

            async def _process_frame(self, frame: int) -> None:
                self.items.append(frame)


        sink = IterableSource(source=[1, 2, 3]) >> Collect()

        await sink.consume_all()
        items = sink.items
        # [1, 2, 3]
    """

    async def consume_all(self) -> None:
        """
        Mount, process every frame until the end of the stream, and unmount.
        """
        await self.mount()
        try:
            with suppress(StopAsyncIteration):
                while True:
                    await self._process_frame(await self.consume_frame())
        finally:
            await self.unmount()

    @abstractmethod
    async def _process_frame(self, frame: Sink_co) -> None:
        raise NotImplementedError()


class BaseStage[Sink_co, Source_co](
    BaseConsumer[Sink_co], BaseProducer[Source_co], ABC
):
    """
    Base class of stages: elements that consume frames and produce transformed frames.

    A stage is both a :class:`BaseConsumer` and a :class:`BaseProducer`, so it fits in the
    middle of a chain: ``source >> stage >> sink``. For every output frame it reads one
    input frame and passes it to ``_process_frame``. A subclass implements
    ``async def _process_frame(self, frame) -> out``. Raise :class:`FrameSkipped` there
    to drop the frame. The end of the upstream stream ends the stage.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseStage
        from pyrill.sources import IterableSource


        class Upper(BaseStage[str, str]):
            async def _process_frame(self, frame: str) -> str:
                return frame.upper()


        stage = IterableSource(source=["a", "b"]) >> Upper()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["A", "B"]
    """

    async def _next_frame(self) -> Source_co:
        return await self._process_frame(await self.consume_frame())

    @abstractmethod
    async def _process_frame(self, frame: Sink_co) -> Source_co:
        raise NotImplementedError()


class BaseDecoupledStage[Sink_co, Source_co](BaseStage[Sink_co, Source_co], ABC):
    """
    Base class of stages that read upstream in a background task, apart from the reader.

    While a :class:`BaseStage` reads one input frame per output frame, a decoupled stage
    starts a task on mount that keeps consuming upstream and hands every frame (or the
    end-of-stream marker, or the exception) to ``_push_frame``. Downstream reads go
    through ``_decoupled_next_frame``. This allows buffering and concurrency, as in
    :class:`pyrill.queues.Queue`.

    A subclass implements three methods: ``_push_frame(frame)``, called by the
    background task with each input frame, ``StopAsyncIteration`` or exception;
    ``_decoupled_next_frame()``, which returns the next output frame or raises what was
    pushed; and ``_process_frame`` (required by :class:`BaseStage`). Unmounting cancels
    the background task.

    Usage:

    .. code-block:: python

        from asyncio import Queue

        from pyrill.base import BaseDecoupledStage
        from pyrill.sources import IterableSource


        class Buffer(BaseDecoupledStage[int, int]):
            def __init__(self) -> None:
                super().__init__()
                self._queue: Queue[int | BaseException] = Queue()

            async def _push_frame(self, frame: int | BaseException) -> None:
                await self._queue.put(frame)

            async def _decoupled_next_frame(self) -> int:
                frame = await self._queue.get()
                if isinstance(frame, BaseException):
                    raise frame
                return frame

            async def _process_frame(self, frame: int) -> int:
                return frame


        stage = IterableSource(source=[1, 2, 3]) >> Buffer()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3]
    """

    _consumer_fut: Future[None] | None = None

    async def _consume(self) -> None:
        try:
            while True:
                await self._push_frame(await self._consume_frame())
        except CancelledError:
            pass
        except StopAsyncIteration as ex:
            await self._push_frame(ex)
        except Exception as ex:  # noqa: BLE001
            await self._push_frame(ex)
            await self._set_error(ex)

    @abstractmethod
    async def _push_frame(self, frame: Sink_co | BaseException) -> None:
        raise NotImplementedError()

    def _ensure_consumer(self) -> None:
        if self._consumer_fut is not None:
            return

        self._consumer_fut = create_task(self._consume(), context=copy_context())

        def cleanup_fut(fut: Future[None]) -> None:
            if fut is self._consumer_fut:
                self._consumer_fut = None

        self._consumer_fut.add_done_callback(cleanup_fut)

    async def _next_frame(self) -> Source_co:
        self._ensure_consumer()
        return await self._decoupled_next_frame()

    @abstractmethod
    async def _decoupled_next_frame(self) -> Source_co:
        raise NotImplementedError()

    async def wait_until_finish_consumer(self) -> None:
        """
        Wait until the background consumer task has finished (no-op if it is not running).
        """
        if self._consumer_fut is None:
            return

        await self._consumer_fut

    async def _mount(self) -> None:
        await super()._mount()
        self._ensure_consumer()

    async def _unmount(self) -> None:
        consumer_fut = self._consumer_fut
        if consumer_fut is not None and consumer_fut is not current_task():
            consumer_fut.cancel()
            # wait() does not re-raise the consumer's own cancellation
            await wait([consumer_fut])

        await super()._unmount()

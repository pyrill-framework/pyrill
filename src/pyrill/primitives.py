from abc import ABC, abstractmethod
from asyncio import FIRST_COMPLETED, FIRST_EXCEPTION, Event, TaskGroup, create_task
from asyncio.futures import Future
from asyncio.tasks import wait
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from contextvars import copy_context
from inspect import isawaitable
from typing import (
    Any,
    Literal,
    Self,
    cast,
    overload,
)

from .base import (
    BUS_MSG_ELEMENT_NULL,
    BaseConsumer,
    BaseDecoupledStage,
    BaseElement,
    BaseProducer,
    BaseSource,
    BaseStage,
    ElementState,
    FrameSkipped,
    Pipeline,
    _ConsumerOf,  # pyright: ignore[reportPrivateUsage]
    active_pipeline,
)
from .mappers import extract_kwargs

__all__ = [
    "BaseBin",
    "Bin",
    "Noop",
    "PrefixStream",
    "SkipIf",
    "SuffixStream",
    "Tee",
    "Zip",
    "bin_stage",
    "zip_sources",
]


class Noop[Frame_co](BaseStage[Frame_co, Frame_co]):
    """
    Stage that passes every frame through unchanged.

    It is the base of stages that only alter the flow of frames, such as
    :class:`PrefixStream`, :class:`SuffixStream` and :class:`Gate`. Subclasses
    override ``_consume_frame`` or ``_next_frame`` to add behaviour.

    Input and output::

        input:  1, 2, 3
        output: 1, 2, 3

    Usage:

    .. code-block:: python

        from pyrill.primitives import Noop
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        stage = source >> Noop[int]()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3]
    """

    async def _process_frame(self, frame: Frame_co) -> Frame_co:
        return frame


class PrefixStream[Frame_co](Noop[Frame_co]):
    """
    Stage that emits ``prefix`` before the frames of its source.

    :param prefix: Frame emitted first, even when the source is empty.

    Input and output::

        input:  "a", "b"
        output: "[", "a", "b"   (prefix="[")
    """

    def __init__(self, *args: Any, prefix: Frame_co, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.prefix = prefix
        self._prefix_pending = True

    async def _mount(self) -> None:
        self._prefix_pending = True
        await super()._mount()

    async def _consume_frame(self) -> Frame_co:
        if self._prefix_pending:
            self._prefix_pending = False
            return self.prefix
        return await super()._consume_frame()


class SuffixStream[Frame_co](Noop[Frame_co]):
    """
    Stage that emits ``suffix`` after the frames of its source.

    :param suffix: Frame emitted last, once the source ends (even when empty).

    Input and output::

        input:  "a", "b"
        output: "a", "b", "]"   (suffix="]")
    """

    def __init__(self, *args: Any, suffix: Frame_co, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.suffix = suffix
        self._suffix_pending = True

    async def _mount(self) -> None:
        self._suffix_pending = True
        await super()._mount()

    async def _consume_frame(self) -> Frame_co:
        try:
            return await super()._consume_frame()
        except StopAsyncIteration:
            if not self._suffix_pending:
                raise
            self._suffix_pending = False
            return self.suffix


class SkipIf[Frame_co](BaseStage[Frame_co, Frame_co]):
    """
    Stage that keeps only the frames accepted by a predicate.

    Despite its name, a frame is passed on when ``check_func`` returns a true
    value and dropped when it returns a false value. ``check_func`` can be a
    regular function or a coroutine function. Dropped frames are skipped without
    ending the stream.

    :param check_func: Function that receives a frame and returns ``True`` to keep
        it or ``False`` to drop it. It may return an awaitable.

    Input and output::

        input:  1, 2, 3, 4, 5
        output: 1, 3, 5   (check_func=lambda x: x % 2 == 1)

    Usage:

    .. code-block:: python

        from pyrill.primitives import SkipIf
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3, 4, 5])
        stage = source >> SkipIf[int](check_func=lambda x: x % 2 == 1)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 3, 5]
    """

    def __init__(
        self,
        *args: Any,
        check_func: Callable[[Frame_co], bool | Awaitable[bool]],
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._check_func = check_func

    async def _process_frame(self, frame: Frame_co) -> Frame_co:
        result: bool | Awaitable[bool] = self._check_func(frame)
        if isawaitable(result):
            result = await result
        if result:
            return frame

        raise FrameSkipped(frame)


async def shielded_push[Source_co](
    consumer: FutureSource[Source_co], frame: Source_co | BaseException
) -> None:
    """
    Feed a frame (or an exception) to a :class:`FutureSource`, ignoring errors.

    :class:`Tee` uses it to push to every branch concurrently, so a branch that
    is already closed cannot break the others.
    """
    with suppress(Exception):
        await consumer.feed(frame)


class FutureSource[Source_co](BaseSource[Source_co]):
    """
    Source whose next frame is supplied from outside through :meth:`feed`.

    Each :class:`Tee` branch is a ``FutureSource``. Reading it waits until a frame
    (or an exception, which is then raised to the reader) is fed. :meth:`feed`
    waits until the reader asks for the next frame, so frames are handed over one
    at a time. It is an internal helper and is not meant to be used directly.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._fut: Future[Source_co | BaseException] | None = None
        self._fut_set_event = Event()

    async def _ensure_ready(self) -> None:
        if self._state is ElementState.NULL:
            await self.mount()

        await super()._ensure_ready()

    async def _next_frame(self) -> Source_co:
        if self._fut is None:
            self._fut = Future[Source_co | BaseException]()
            self._fut_set_event.set()
        result = await self._fut
        if isinstance(result, BaseException):
            raise result
        return result

    async def feed(self, frame: Source_co | BaseException) -> None:
        """
        Hand ``frame`` to the reader once it asks for one.

        An exception instance is raised in the reader instead of being returned.
        Raises :class:`RuntimeError` if the source is closed while waiting.
        """
        while self._fut is None:
            if self._closed:
                raise RuntimeError("Consumer is closed")
            await self._fut_set_event.wait()

        self._fut.set_result(frame)
        self._fut = None
        self._fut_set_event.clear()

    async def _mount(self) -> None:
        self._fut = None
        self._fut_set_event.clear()
        await super()._mount()

    async def _unmount(self) -> None:
        await super()._unmount()
        # Wake any pending feed() so it sees the consumer is closed.
        self._fut_set_event.set()


class Tee[Source_co](BaseDecoupledStage[Source_co, Source_co]):
    """
    Stage that fans out its frames: every connected consumer gets every frame.

    Connect one consumer with ``tee >> consumer`` or several at once with
    ``tee >> (consumer_1, consumer_2)``, which returns the tuple of the consumers
    so it can be followed by :class:`Zip`. Each consumer has its own branch and
    must be read concurrently with the others (for example with
    ``asyncio.gather``). Frames are pushed to all branches together, so the
    slowest branch sets the pace. A branch that finishes early (for example
    :class:`~pyrill.sinks.First`) is removed and the others keep running. When
    the last branch is gone, or the source ends, the tee unmounts. Iterating the
    tee directly is not supported; use ``aiter(tee)`` or ``>>``.

    Input and output::

        input:  1, 2, 3
        branch 1: 1, 2, 3
        branch 2: 1, 2, 3

    Usage:

    .. code-block:: python

        import asyncio

        from pyrill.mappers import Map
        from pyrill.primitives import Tee
        from pyrill.sinks import Last
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        tens, texts = source >> Tee[int]() >> (
            Map[int, int](map_func=lambda frame: frame * 10),
            Map[int, str](map_func=str),
        )
        last_tens = tens >> Last[int]()
        last_texts = texts >> Last[str]()

        results = await asyncio.gather(last_tens.get_frame(), last_texts.get_frame())
        # [30, "3"]
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._consumer_srcs: set[FutureSource[Source_co]] = set()

    async def _push_frame(self, frame: Source_co | BaseException) -> None:
        async with TaskGroup() as tg:
            for consumer_src in self._consumer_srcs:
                tg.create_task(
                    shielded_push(consumer_src, frame),
                    name=consumer_src.name,
                    context=copy_context(),
                )

    def __aiter__(self) -> AsyncIterator[Source_co]:
        return self._create_consumer()

    def _create_consumer(
        self,
    ) -> BaseProducer[Source_co]:
        src = FutureSource[Source_co]()
        self._consumer_srcs.add(src)

        self._ensure_consumer()

        src.add_message_callback(
            lambda src, message: self._remove_consumer(
                cast(FutureSource[Source_co], src)
            ),
            BUS_MSG_ELEMENT_NULL,
        )

        return src

    def _remove_consumer(self, consumer: FutureSource[Source_co]) -> None:
        self._consumer_srcs.discard(consumer)
        if len(self._consumer_srcs) == 0:
            self.pipeline.create_orphan_task(self.unmount())

    async def _process_frame(self, frame: Source_co) -> Source_co:
        return frame

    async def _decoupled_next_frame(self) -> Source_co:
        raise RuntimeError(
            "Tee does not support direct __anext__; iterate consumers created via aiter(tee)"
        )

    def consumer_count(self) -> int:
        """
        Return the number of branches currently connected to the tee.
        """
        return len(self._consumer_srcs)

    # ``tee >> consumer`` connects one branch; ``tee >> (c1, c2)`` connects every
    # consumer in the tuple and returns the tuple, so ``>> Zip()`` can follow.
    # Accepting tuples widens BaseProducer.__rshift__, which mypy rejects for
    # operators; only Tee can fan out, so the base class must not accept them.
    @overload  # type: ignore[override]
    def __rshift__[R](self, other: _ConsumerOf[Source_co, R]) -> R: ...

    @overload
    def __rshift__[R1](
        self, other: tuple[_ConsumerOf[Source_co, R1],]
    ) -> tuple[R1]: ...

    @overload
    def __rshift__[R1, R2](
        self, other: tuple[_ConsumerOf[Source_co, R1], _ConsumerOf[Source_co, R2]]
    ) -> tuple[R1, R2]: ...

    @overload
    def __rshift__[R1, R2, R3](
        self,
        other: tuple[
            _ConsumerOf[Source_co, R1],
            _ConsumerOf[Source_co, R2],
            _ConsumerOf[Source_co, R3],
        ],
    ) -> tuple[R1, R2, R3]: ...

    @overload
    def __rshift__[R1, R2, R3, R4](
        self,
        other: tuple[
            _ConsumerOf[Source_co, R1],
            _ConsumerOf[Source_co, R2],
            _ConsumerOf[Source_co, R3],
            _ConsumerOf[Source_co, R4],
        ],
    ) -> tuple[R1, R2, R3, R4]: ...

    @overload
    def __rshift__[R1, R2, R3, R4, R5](
        self,
        other: tuple[
            _ConsumerOf[Source_co, R1],
            _ConsumerOf[Source_co, R2],
            _ConsumerOf[Source_co, R3],
            _ConsumerOf[Source_co, R4],
            _ConsumerOf[Source_co, R5],
        ],
    ) -> tuple[R1, R2, R3, R4, R5]: ...

    @overload
    def __rshift__(
        self,
        other: tuple[
            _ConsumerOf[Source_co, Any],
            _ConsumerOf[Source_co, Any],
            _ConsumerOf[Source_co, Any],
            _ConsumerOf[Source_co, Any],
            _ConsumerOf[Source_co, Any],
            _ConsumerOf[Source_co, Any],
            *tuple[_ConsumerOf[Source_co, Any], ...],
        ],
    ) -> tuple[Any, ...]: ...

    def __rshift__(self, other: Any) -> Any:
        if not isinstance(other, tuple):
            return super().__rshift__(other)

        consumers = cast(tuple[object, ...], other)
        if not all(isinstance(consumer, BaseConsumer) for consumer in consumers):
            return NotImplemented
        # Check every consumer first so a bad one leaves nothing connected.
        for consumer in cast(tuple[BaseConsumer[Source_co], ...], consumers):
            if consumer.pipeline is not self.pipeline:
                raise RuntimeError("Cannot connect elements from different pipelines")
        for consumer in cast(tuple[BaseConsumer[Source_co], ...], consumers):
            consumer.connect(self)
        return consumers

    async def _mount(self) -> None:
        # Do not start _consume() eagerly at mount time; _create_consumer() starts
        # it lazily when the first consumer registers, avoiding a race where the
        # source is fully drained before any FutureSource is registered.
        async with TaskGroup() as tg:
            for consumer_src in self._consumer_srcs:
                tg.create_task(
                    consumer_src.mount(),
                    name=consumer_src.name,
                    context=copy_context(),
                )
        await super(BaseDecoupledStage, self)._mount()

    async def _unmount(self) -> None:
        async with TaskGroup() as tg:
            for consumer_src in self._consumer_srcs:
                tg.create_task(
                    shielded_push(consumer_src, StopAsyncIteration()),
                    name=consumer_src.name,
                    context=copy_context(),
                )

        async with TaskGroup() as tg:
            for consumer_src in self._consumer_srcs:
                tg.create_task(
                    consumer_src.unmount(),
                    name=consumer_src.name,
                    context=copy_context(),
                )

        await super()._unmount()


class Union[Source_co](BaseProducer[Source_co]):
    """
    Producer that merges several sources into a single stream.

    Sources are added with :meth:`add_source` and are read concurrently; frames
    come out in the order they become available, so the order between different
    sources is not deterministic. The stream ends as soon as any source is
    exhausted, and frames still pending in the other sources are dropped. With
    no sources the stream ends immediately. An exception raised by a source is
    re-raised downstream. Sources must belong to the same pipeline as the union.

    Input and output::

        sources: [1, 2, 3]
        output:  1, 2, 3

    Usage:

    .. code-block:: python

        from pyrill.primitives import Union
        from pyrill.sources import IterableSource

        union = Union[int]()
        union.add_source(IterableSource(source=[1, 2, 3]))

        await union.mount()
        frames = [frame async for frame in union]
        # [1, 2, 3]
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._sources: dict[BaseProducer[Source_co], Future[Source_co] | None] = {}

    def add_source(self, source: BaseProducer[Source_co]) -> None:
        """
        Add a source to the union.

        Raises :class:`RuntimeError` if the source belongs to another pipeline or the
        union is unmounting or in error state.
        """
        if source.pipeline is not self.pipeline:
            raise RuntimeError(
                "Source must belong to the same pipeline as the aggregator"
            )

        if self.state in [ElementState.UNMOUNTING, ElementState.ERROR]:
            raise RuntimeError(
                "Cannot add source while aggregator is unmounting or in error state"
            )

        self._sources[source] = None

    def remove_source(self, source: BaseProducer[Source_co]) -> None:
        """
        Remove a source from the union, cancelling its pending read, if any.

        Removing a source that was never added does nothing.
        """
        fut = self._sources.pop(source, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def source_count(self) -> int:
        """
        Return the number of sources currently in the union.
        """
        return len(self._sources)

    async def _mount(self) -> None:
        for source in self._sources:
            await source.mount()
        await super()._mount()

    async def _unmount(self) -> None:
        for src, fut in list(self._sources.items()):
            await src.unmount()
            if fut is None:
                continue
            if not fut.done():
                fut.cancel()

            with suppress(BaseException):
                await fut
            self._sources[src] = None
        await super()._unmount()

    async def _next_frame(self) -> Source_co:
        if not self._sources:
            raise StopAsyncIteration()

        for source in set(self._sources.keys()):
            if self._sources[source] is not None:
                continue
            self._sources[source] = create_task(source.__anext__())

        done, _ = await wait(
            [fut for fut in self._sources.values() if fut is not None],
            return_when=FIRST_COMPLETED,
        )

        result_fut = done.pop()
        for source, fut in self._sources.items():
            if fut is result_fut:
                self._sources[source] = None
                break

        return result_fut.result()


class Zip[*Ts = *tuple[Any, ...]](BaseProducer[tuple[*Ts]]):
    """
    Producer that combines several sources into tuples, one frame from each.

    Items are ordered as the sources were added. By default the stream ends as
    soon as any source is exhausted. With ``exhausted_as_none=True`` an exhausted
    source contributes ``None`` instead, and the stream ends once every source is
    exhausted. An exception raised by any source is re-raised downstream.

    The type parameters give the item type of each position, for example
    ``Zip[int, str]`` produces ``tuple[int, str]``; with ``exhausted_as_none=True``
    declare them optional, as in ``Zip[int, str | None]``. Type checkers cannot
    match sources to positions, so they are accepted as ``BaseProducer[Any]``.
    A bare ``Zip()`` produces ``tuple[Any, ...]``.

    :param exhausted_as_none: Emit ``None`` for exhausted sources instead of
        ending the stream.

    Input and output::

        sources: [1, 2, 3], ["a", "b"]
        output:  (1, "a"), (2, "b")
        output with exhausted_as_none=True: (1, "a"), (2, "b"), (3, None)

    Usage:

    .. code-block:: python

        from pyrill.primitives import Zip
        from pyrill.sources import IterableSource

        numbers = IterableSource(source=[1, 2, 3])
        tens = IterableSource(source=[10])
        zipped = (numbers, tens) >> Zip[int | None, int | None](
            exhausted_as_none=True
        )

        await zipped.mount()
        frames = [frame async for frame in zipped]
        # [(1, 10), (2, None), (3, None)]

    Sources can also be connected with ``source1 >> Zip() << source2``,
    ``Zip() << (source1, source2)`` or :meth:`add_source`.
    """

    def __init__(self, *args: Any, exhausted_as_none: bool = False, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.exhausted_as_none = exhausted_as_none
        self._sources: list[BaseProducer[Any]] = []
        self._exhausted: set[BaseProducer[Any]] = set()

    def _check_source(self, source: BaseProducer[Any]) -> None:
        if source.pipeline is not self.pipeline:
            raise RuntimeError("Source must belong to the same pipeline as the zip")

        if self.state in [ElementState.UNMOUNTING, ElementState.ERROR]:
            raise RuntimeError(
                "Cannot add source while zip is unmounting or in error state"
            )

    def add_source(self, source: BaseProducer[Any]) -> None:
        self._check_source(source)
        self._sources.append(source)

    def _connect(
        self, other: BaseProducer[Any] | tuple[BaseProducer[Any], ...]
    ) -> bool:
        sources = other if isinstance(other, tuple) else (other,)
        if not all(isinstance(source, BaseProducer) for source in sources):  # pyright: ignore[reportUnnecessaryIsInstance]
            return False
        # Check every source first so a bad one leaves the zip unchanged.
        for source in sources:
            self._check_source(source)
        self._sources.extend(sources)
        return True

    def remove_source(self, source: BaseProducer[Any]) -> None:
        with suppress(ValueError):
            self._sources.remove(source)
        self._exhausted.discard(source)

    def source_count(self) -> int:
        return len(self._sources)

    # ``zip << source`` and ``zip << (source1, source2)`` add sources and return
    # the zip, so ``source1 >> Zip() << source2`` chains.
    def __lshift__(
        self, other: BaseProducer[Any] | tuple[BaseProducer[Any], ...]
    ) -> Self:
        if not self._connect(other):
            return NotImplemented
        return self

    # ``source >> zip`` and ``(source1, source2) >> zip`` add sources.
    # BaseProducer.__rshift__ delegates here, so both paths return the same type.
    __rrshift__ = __lshift__

    async def _mount(self) -> None:
        self._exhausted.clear()
        for source in self._sources:
            await source.mount()
        await super()._mount()

    async def _unmount(self) -> None:
        for source in self._sources:
            await source.unmount()
        self._exhausted.clear()
        await super()._unmount()

    async def _next_frame(self) -> tuple[*Ts]:
        active = [s for s in self._sources if s not in self._exhausted]
        if not active:
            raise StopAsyncIteration()

        tasks = {create_task(source.__anext__()): source for source in active}
        values: dict[BaseProducer[Any], Any] = {}
        pending = set(tasks)
        try:
            while pending:
                done, pending = await wait(pending, return_when=FIRST_EXCEPTION)
                for task in done:
                    source = tasks[task]
                    ex = task.exception()
                    if ex is None:
                        values[source] = task.result()
                    elif isinstance(ex, StopAsyncIteration) and self.exhausted_as_none:
                        self._exhausted.add(source)
                    else:
                        raise ex
        finally:
            for task in pending:
                task.cancel()
            if pending:
                await wait(pending)
            # Mark every finished task's exception as retrieved.
            for task in tasks:
                if task.done() and not task.cancelled():
                    task.exception()

        if len(self._exhausted) == len(self._sources):
            raise StopAsyncIteration()

        return cast(tuple[*Ts], tuple(values.get(source) for source in self._sources))


@overload
def zip_sources[A](
    s1: BaseProducer[A],
    /,
    *,
    exhausted_as_none: Literal[False] = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A]: ...


@overload
def zip_sources[A](
    s1: BaseProducer[A],
    /,
    *,
    exhausted_as_none: bool,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A | None]: ...


@overload
def zip_sources[A, B](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    /,
    *,
    exhausted_as_none: Literal[False] = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A, B]: ...


@overload
def zip_sources[A, B](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    /,
    *,
    exhausted_as_none: bool,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A | None, B | None]: ...


@overload
def zip_sources[A, B, C](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    s3: BaseProducer[C],
    /,
    *,
    exhausted_as_none: Literal[False] = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A, B, C]: ...


@overload
def zip_sources[A, B, C](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    s3: BaseProducer[C],
    /,
    *,
    exhausted_as_none: bool,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A | None, B | None, C | None]: ...


@overload
def zip_sources[A, B, C, D](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    s3: BaseProducer[C],
    s4: BaseProducer[D],
    /,
    *,
    exhausted_as_none: Literal[False] = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A, B, C, D]: ...


@overload
def zip_sources[A, B, C, D](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    s3: BaseProducer[C],
    s4: BaseProducer[D],
    /,
    *,
    exhausted_as_none: bool,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A | None, B | None, C | None, D | None]: ...


@overload
def zip_sources[A, B, C, D, E](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    s3: BaseProducer[C],
    s4: BaseProducer[D],
    s5: BaseProducer[E],
    /,
    *,
    exhausted_as_none: Literal[False] = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A, B, C, D, E]: ...


@overload
def zip_sources[A, B, C, D, E](
    s1: BaseProducer[A],
    s2: BaseProducer[B],
    s3: BaseProducer[C],
    s4: BaseProducer[D],
    s5: BaseProducer[E],
    /,
    *,
    exhausted_as_none: bool,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[A | None, B | None, C | None, D | None, E | None]: ...


# Only six or more sources fall back to Any: if this overload also accepted
# fewer, mypy would pick it whenever a precise overload failed to match.
@overload
def zip_sources(
    s1: BaseProducer[Any],
    s2: BaseProducer[Any],
    s3: BaseProducer[Any],
    s4: BaseProducer[Any],
    s5: BaseProducer[Any],
    s6: BaseProducer[Any],
    /,
    *sources: BaseProducer[Any],
    exhausted_as_none: bool = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[*tuple[Any, ...]]: ...


def zip_sources(
    *sources: BaseProducer[Any],
    exhausted_as_none: bool = False,
    name: str | None = None,
    pipeline: Pipeline | None = None,
) -> Zip[*tuple[Any, ...]]:
    """
    Build a :class:`Zip` over ``sources`` with each tuple position typed from its
    source: ``zip_sources(numbers, names)`` is a ``Zip[int, str]``. Unless
    ``exhausted_as_none`` is the literal ``False`` (the default), every position
    is typed optional. Up to five sources are typed precisely; more fall back to
    ``Zip[*tuple[Any, ...]]``.

    :param exhausted_as_none: Emit ``None`` for exhausted sources instead of
        ending the stream.
    :param name: Element name.
    :param pipeline: Pipeline for the zip. Defaults to the first source's.
    """
    if pipeline is None and sources:
        pipeline = sources[0].pipeline
    zipped = Zip[*tuple[Any, ...]](
        name=name, pipeline=pipeline, exhausted_as_none=exhausted_as_none
    )
    return zipped << sources


class _BinInlet[Frame_co](BaseProducer[Frame_co]):
    """Feeds the frames a :class:`Bin` consumes to the first element inside it."""

    def __init__(
        self, *args: Any, pull: Callable[[], Awaitable[Frame_co]], **kwargs: Any
    ):
        super().__init__(*args, **kwargs)
        self._pull = pull

    async def _next_frame(self) -> Frame_co:
        return await self._pull()


class BaseBin[Sink_co, Source_co](BaseConsumer[Sink_co], BaseProducer[Source_co], ABC):
    """
    Base for stages built by composition: a container for inner elements.

    Subclasses implement :meth:`_get_upstream_downstream`, returning the first
    inner element (the upstream consumer, fed with the frames the bin consumes)
    and the last one (the downstream producer, whose frames the bin produces).
    It is called on the first mount, so subclasses can use attributes set in
    their ``__init__``; the inner elements are then reused on every remount.
    Mounting, unmounting and errors propagate through the inner elements, so the
    bin behaves like any other stage.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseConsumer, BaseProducer
        from pyrill.primitives import BaseBin
        from pyrill.sources import IterableSource
        from pyrill.strings import Upper
        from pyrill.strlike import Strip


        class Shout(BaseBin[str, str]):
            def _get_upstream_downstream(
                self,
            ) -> tuple[BaseConsumer[str], BaseProducer[str]]:
                strip = Strip[str]()
                return strip, strip >> Upper()


        stage = IterableSource(source=[" hi ", " there"]) >> Shout()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["HI", "THERE"]
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._inlet = _BinInlet[Sink_co](
            name=f"{self.name}-inlet", pipeline=self.pipeline, pull=self._pull_frame
        )
        self._inner: tuple[BaseConsumer[Sink_co], BaseProducer[Source_co]] | None = None
        self._outlet: AsyncIterator[Source_co] | None = None

    @abstractmethod
    def _get_upstream_downstream(
        self,
    ) -> tuple[BaseConsumer[Sink_co], BaseProducer[Source_co]]:
        raise NotImplementedError()

    @property
    def upstream_consumer(self) -> BaseConsumer[Sink_co] | None:
        return None if self._inner is None else self._inner[0]

    @property
    def downstream_producer(self) -> BaseProducer[Source_co] | None:
        return None if self._inner is None else self._inner[1]

    def _check_inner(self, element: BaseElement) -> None:
        if element.pipeline is not self.pipeline:
            raise RuntimeError("Inner element must belong to the bin's pipeline")

    def _build_inner(self) -> tuple[BaseConsumer[Sink_co], BaseProducer[Source_co]]:
        if self._inner is None:
            # Elements created by the builder join the bin's pipeline by default.
            token = active_pipeline.set(self.pipeline)
            try:
                upstream, downstream = self._get_upstream_downstream()
            finally:
                active_pipeline.reset(token)
            self._check_inner(upstream)
            self._check_inner(downstream)
            upstream.connect(self._inlet)
            self._inner = (upstream, downstream)
        return self._inner

    async def _pull_frame(self) -> Sink_co:
        # No state check: inner elements may start pulling while the bin mounts.
        if self._iter is None:
            raise StopAsyncIteration()
        return await anext(self._iter)

    async def _mount(self) -> None:
        _, downstream = self._build_inner()
        await super()._mount()
        await downstream.mount()
        self._outlet = aiter(downstream)

    async def _unmount(self) -> None:
        self._outlet = None
        with suppress(Exception):
            if self._inner is not None:
                await self._inner[1].unmount()
        await super()._unmount()

    async def _next_frame(self) -> Source_co:
        if self._outlet is None:
            raise RuntimeError("Bin is not mounted")
        return await anext(self._outlet)


class Bin[Sink_co, Source_co](BaseBin[Sink_co, Source_co]):
    """
    :class:`BaseBin` whose inner elements are given rather than built.

    :param upstream: First inner element; see :meth:`set_upstream_consumer`.
    :param downstream: Last inner element; see :meth:`set_downstream_producer`.

    Usage:

    .. code-block:: python

        from pyrill.primitives import Bin
        from pyrill.sources import IterableSource
        from pyrill.strings import Upper
        from pyrill.strlike import Strip

        source = IterableSource(source=[" hi ", " there"])
        strip = Strip[str]()
        stage = source >> Bin[str, str](upstream=strip, downstream=strip >> Upper())

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["HI", "THERE"]

    Subclasses can also call the setters in ``__init__``; to build the inner
    elements in a method instead, subclass :class:`BaseBin`.
    """

    def __init__(
        self,
        *args: Any,
        upstream: BaseConsumer[Sink_co] | None = None,
        downstream: BaseProducer[Source_co] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._upstream: BaseConsumer[Sink_co] | None = None
        self._downstream: BaseProducer[Source_co] | None = None

        if upstream is not None:
            self.set_upstream_consumer(upstream)
        if downstream is not None:
            self.set_downstream_producer(downstream)

    @property
    def upstream_consumer(self) -> BaseConsumer[Sink_co] | None:
        return self._upstream

    @property
    def downstream_producer(self) -> BaseProducer[Source_co] | None:
        return self._downstream

    def _check_inner(self, element: BaseElement) -> None:
        super()._check_inner(element)
        if self.state is ElementState.READY:
            raise RuntimeError("Unmount the bin before changing its inner elements")

    def set_upstream_consumer(self, consumer: BaseConsumer[Sink_co]) -> None:
        self._check_inner(consumer)
        consumer.connect(self._inlet)
        self._upstream = consumer
        self._inner = None

    def set_downstream_producer(self, producer: BaseProducer[Source_co]) -> None:
        self._check_inner(producer)
        self._downstream = producer
        self._inner = None

    def _get_upstream_downstream(
        self,
    ) -> tuple[BaseConsumer[Sink_co], BaseProducer[Source_co]]:
        if self._upstream is None or self._downstream is None:
            raise RuntimeError(
                "Bin needs an upstream consumer and a downstream producer"
            )
        return self._upstream, self._downstream


type BinBuilder[Sink_co, Source_co] = Callable[
    ..., tuple[BaseConsumer[Sink_co], BaseProducer[Source_co]]
]


@overload
def bin_stage[Sink_co, Source_co](
    func: BinBuilder[Sink_co, Source_co], /
) -> type[BaseBin[Sink_co, Source_co]]: ...


@overload
def bin_stage[Sink_co, Source_co](
    func: None = None, /, *, name: str | None = None
) -> Callable[[BinBuilder[Sink_co, Source_co]], type[BaseBin[Sink_co, Source_co]]]: ...


def bin_stage[Sink_co, Source_co](
    func: BinBuilder[Sink_co, Source_co] | None = None,
    /,
    *,
    name: str | None = None,
) -> (
    type[BaseBin[Sink_co, Source_co]]
    | Callable[[BinBuilder[Sink_co, Source_co]], type[BaseBin[Sink_co, Source_co]]]
):
    """
    Decorator turning a builder function into a :class:`BaseBin` subclass.

    The builder returns the bin's upstream consumer and downstream producer. Each
    instance of the returned class calls it once (on its first mount), so every
    bin gets its own inner elements. Keyword-only parameters of the builder
    become keyword arguments of the class.

    :param name: Class name, also used as the prefix of element names. Defaults
        to the builder's name.

    Usage:

    .. code-block:: python

        from pyrill.base import BaseConsumer, BaseProducer
        from pyrill.mappers import Map
        from pyrill.primitives import bin_stage
        from pyrill.sources import IterableSource
        from pyrill.strlike import Strip


        @bin_stage(name="Suffixed")
        def suffixed(*, suffix: str) -> tuple[BaseConsumer[str], BaseProducer[str]]:
            strip = Strip[str]()
            return strip, strip >> Map[str, str](map_func=lambda f: f + suffix)


        stage = IterableSource(source=[" a ", " b"]) >> suffixed(suffix="!")

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["a!", "b!"]
    """

    def inner(
        func: BinBuilder[Sink_co, Source_co],
    ) -> type[BaseBin[Sink_co, Source_co]]:
        class BuiltBin(BaseBin[Sink_co, Source_co]):
            def __init__(self, *args: Any, **kwargs: Any):
                self._builder_kwargs = extract_kwargs(kwargs, func)

                super().__init__(*args, **kwargs)

            def _get_upstream_downstream(
                self,
            ) -> tuple[BaseConsumer[Sink_co], BaseProducer[Source_co]]:
                return func(**self._builder_kwargs)

        BuiltBin.__doc__ = func.__doc__
        BuiltBin.__module__ = func.__module__
        BuiltBin.__name__ = BuiltBin.__qualname__ = name or func.__name__
        return BuiltBin

    return inner(func) if func is not None else inner


class BaseFlat[Sink_co, Source_co](BaseStage[BaseProducer[Sink_co], Source_co], ABC):
    """
    Base for stages that consume producers and emit the frames they produce.

    The stage reads one producer from its source, mounts it and emits its frames
    one by one, passing each through :meth:`_flat_process_frame`. When the inner
    producer ends, it is unmounted and the next one is read; an empty producer
    is skipped. The stream ends when the source ends. Subclasses must implement
    ``_flat_process_frame(frame)``, which receives each inner frame and returns
    the frame to emit.

    Input and output::

        input:  producer([1, 2]), producer([3])
        output: 10, 20, 30   (subclass that multiplies by 10)

    Usage:

    .. code-block:: python

        from pyrill.primitives import BaseFlat
        from pyrill.sources import IterableSource


        class Times10(BaseFlat[int, int]):
            async def _flat_process_frame(self, frame: int) -> int:
                return frame * 10


        inner_1 = IterableSource(source=[1, 2])
        inner_2 = IterableSource(source=[3])
        stage = IterableSource(source=[inner_1, inner_2]) >> Times10()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [10, 20, 30]
    """

    _current_frame: BaseProducer[Sink_co] | None = None

    async def _next_frame(self) -> Source_co:
        if self._current_frame is None:
            self._current_frame = await self.consume_frame()
            await self._current_frame.mount()

        try:
            return await self._process_frame(self._current_frame)
        except StopAsyncIteration as ex:
            await self._current_frame.unmount()
            self._current_frame = None
            raise FrameSkipped(ex)

    async def _process_frame(self, frame: BaseProducer[Sink_co]) -> Source_co:
        return await self._flat_process_frame(await frame.__anext__())

    @abstractmethod
    async def _flat_process_frame(self, frame: Sink_co) -> Source_co:
        raise NotImplementedError()


class Flat[Source_co](BaseFlat[Source_co, Source_co]):
    """
    Stage that flattens a stream of producers into a stream of their frames.

    Each input frame is a producer (for example an
    :class:`~pyrill.sources.IterableSource`). The frames of the first producer
    are emitted, then those of the second, and so on, in order. Empty producers
    are skipped.

    Input and output::

        input:  producer([1, 2]), producer([]), producer([3, 4])
        output: 1, 2, 3, 4

    Usage:

    .. code-block:: python

        from pyrill.primitives import Flat
        from pyrill.sources import IterableSource

        inner_1 = IterableSource(source=[1, 2])
        inner_2 = IterableSource(source=[3, 4])
        stage = IterableSource(source=[inner_1, inner_2]) >> Flat[int]()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3, 4]
    """

    async def _flat_process_frame(self, frame: Source_co) -> Source_co:
        return frame


class Retry[Source_co](Flat[Source_co]):
    """
    Stage that flattens producers and survives failures of the inner producers.

    It behaves like :class:`Flat`. When the current inner producer raises one of
    ``retry_exceptions``, that producer is unmounted and discarded (frames it
    already delivered are kept) and the stage moves on to the next producer from
    its source, which is how a failed attempt is retried. Once ``max_retries``
    consecutive failures have happened, the next error is raised. The counter is
    reset after every successful frame and on every mount.

    :param max_retries: Number of consecutive failures tolerated (default ``1``).
    :param retry_exceptions: Exception types that trigger a retry. Defaults to
        ``(Exception,)``. Other exceptions are raised immediately.

    Input and output::

        input:  producer(1, then fails), producer([2, 3])
        output: 1, 2, 3   (max_retries=1)

    Usage:

    .. code-block:: python

        from collections.abc import Iterator

        from pyrill.primitives import Retry
        from pyrill.sources import IterableSource


        def flaky() -> Iterator[int]:
            yield 1
            raise ValueError("temporary failure")


        attempts = [IterableSource(source=flaky()), IterableSource(source=[2, 3])]
        stage = IterableSource(source=attempts) >> Retry[int](max_retries=1)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 3]
    """

    def __init__(
        self,
        *args: Any,
        max_retries: int = 1,
        retry_exceptions: tuple[type[Exception], ...] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._current_retry = 0
        self._max_retries = max_retries
        self._retry_exceptions = retry_exceptions or (Exception,)

    async def _next_frame(self) -> Source_co:
        try:
            result = await super()._next_frame()
            self._current_retry = 0
            return result
        except FrameSkipped, StopAsyncIteration:
            raise
        except self._retry_exceptions as ex:
            if self._current_frame is not None:
                await self._current_frame.unmount()
            self._current_frame = None
            if self._current_retry >= self._max_retries:
                raise
            self._current_retry += 1
            raise FrameSkipped(ex)

    async def _mount(self) -> None:
        self._current_retry = 0
        await super()._mount()


type MapFunc[Sink_co, Source_co] = Callable[
    [Sink_co], BaseProducer[Source_co] | Awaitable[BaseProducer[Source_co]]
]


class BaseFlatMap[Sink_co, Source_co](BaseStage[Sink_co, Source_co], ABC):
    """
    Base for stages that map each frame to a producer and emit its frames.

    For every input frame the stage calls ``_map_frame_to_producer(frame)``,
    mounts the producer it returns, emits all of its frames and unmounts it
    before reading the next input frame. Producers that yield nothing are
    skipped. Subclasses must implement ``async _map_frame_to_producer(frame)``,
    returning a :class:`~pyrill.base.BaseProducer`. See :class:`FlatMap` for a
    ready-made version based on a function.

    Input and output::

        input:  1, 2
        output: 1, 2, 2   (producer yields the frame as many times as its value)

    Usage:

    .. code-block:: python

        from pyrill.base import BaseProducer
        from pyrill.primitives import BaseFlatMap
        from pyrill.sources import IterableSource


        class Repeat(BaseFlatMap[int, int]):
            async def _map_frame_to_producer(self, frame: int) -> BaseProducer[int]:
                return IterableSource(source=[frame] * frame)


        stage = IterableSource(source=[1, 2]) >> Repeat()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 2]
    """

    _current_frame: BaseProducer[Source_co] | None = None

    async def _next_frame(self) -> Source_co:
        if self._current_frame is None:
            self._current_frame = await self._map_frame_to_producer(
                await self.consume_frame()
            )
            await self._current_frame.mount()

        try:
            return await anext(self._current_frame)
        except StopAsyncIteration as ex:
            await self._current_frame.unmount()
            self._current_frame = None
            raise FrameSkipped(ex)

    @abstractmethod
    async def _map_frame_to_producer(self, frame: Sink_co) -> BaseProducer[Source_co]:
        raise NotImplementedError()

    async def _process_frame(self, frame: Sink_co) -> Source_co:
        raise NotImplementedError()


class FlatMap[Sink_co, Source_co](BaseFlatMap[Sink_co, Source_co]):
    """
    Stage that maps each frame to a producer and emits the frames of them all.

    It is a one-to-many mapper: ``map_func`` turns each input frame into a
    producer (or returns an awaitable of one), and the frames of that producer
    are emitted before the next input frame is read. The decorator
    :func:`flat_map` builds this stage from a function.

    :param map_func: Function from a frame to a producer, or to an awaitable
        producer.

    Input and output::

        input:  1, 2, 3
        output: 1, 2, 2, 3, 3, 3   (each frame repeated as many times as its value)

    Usage:

    .. code-block:: python

        from pyrill.primitives import FlatMap
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        stage = source >> FlatMap[int, int](
            map_func=lambda frame: IterableSource(source=[frame] * frame)
        )

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 2, 3, 3, 3]
    """

    def __init__(
        self, *args: Any, map_func: MapFunc[Sink_co, Source_co], **kwargs: Any
    ) -> None:
        self._map_func = map_func
        super().__init__(*args, **kwargs)

    async def _map_frame_to_producer(self, frame: Sink_co) -> BaseProducer[Source_co]:
        result = self._map_func(frame)
        if isinstance(result, Awaitable):
            result = await result
        return result


@overload
def flat_map[Sink_co, Source_co](
    name: str | None,
    func: MapFunc[Sink_co, Source_co],
) -> BaseFlatMap[Sink_co, Source_co]: ...


@overload
def flat_map[Sink_co, Source_co](
    name: str | None = None,
    func: None = None,
) -> Callable[[MapFunc[Sink_co, Source_co]], BaseFlatMap[Sink_co, Source_co]]: ...


def flat_map[Sink_co, Source_co](
    name: str | None = None,
    func: MapFunc[Sink_co, Source_co] | None = None,
) -> (
    BaseFlatMap[Sink_co, Source_co]
    | Callable[[MapFunc[Sink_co, Source_co]], BaseFlatMap[Sink_co, Source_co]]
):
    """
    Build a :class:`FlatMap` stage from a function, usually as a decorator.

    Use ``@flat_map()`` (note the parentheses) or ``@flat_map("name")`` to
    decorate a function that maps a frame to a producer; the result is a stage
    instance named after the function unless ``name`` is given. The function can
    be a coroutine function. ``flat_map("name", func)`` builds the stage
    directly. The bare form ``@flat_map`` is not supported because the function
    would be taken as ``name``.

    :param name: Name of the stage. Defaults to the function name.
    :param func: Function to wrap. When omitted, a decorator is returned.

    Input and output::

        input:  1, 2, 3
        output: 1, 2, 2, 3, 3, 3

    Usage:

    .. code-block:: python

        from pyrill.base import BaseProducer
        from pyrill.primitives import flat_map
        from pyrill.sources import IterableSource


        @flat_map()
        def repeat(frame: int) -> BaseProducer[int]:
            return IterableSource(source=[frame] * frame)


        stage = IterableSource(source=[1, 2, 3]) >> repeat

        await stage.mount()
        frames = [frame async for frame in stage]
        # [1, 2, 2, 3, 3, 3]
    """

    def inner(func: MapFunc[Sink_co, Source_co]) -> BaseFlatMap[Sink_co, Source_co]:
        return FlatMap(name=name or func.__name__, map_func=func)

    return inner(func) if func is not None else inner


class Gate[Source_co](Noop[Source_co]):
    """
    Stage that holds frames back until it is opened.

    The gate starts closed. While it is closed, reading blocks and the source is
    not pulled. Call :meth:`open` to let frames flow and :meth:`close` to hold
    them again. The gate is checked before a frame is pulled and again before it
    is returned, so a frame already pulled when the gate closes is delivered only
    after it reopens. Use it to pause and resume a stream from another task.

    Input and output::

        input:  1, 2, 3
        output: 1, 2, 3   (delivered only while the gate is open)

    Usage:

    .. code-block:: python

        import asyncio

        from pyrill.primitives import Gate
        from pyrill.sources import IterableSource

        source = IterableSource(source=[1, 2, 3])
        gate = source >> Gate[int]()


        async def read() -> list[int]:
            await gate.mount()
            return [frame async for frame in gate]


        async def open_later() -> None:
            await asyncio.sleep(0.01)
            gate.open()


        frames = (await asyncio.gather(read(), open_later()))[0]
        # [1, 2, 3]
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._gate = Event()
        super().__init__(*args, **kwargs)

    def open(self) -> None:
        """
        Open the gate, releasing any blocked reader.
        """
        self._gate.set()

    def close(self) -> None:
        """
        Close the gate: readers block until it is opened again.
        """
        self._gate.clear()

    async def _next_frame(self) -> Source_co:
        await self._gate.wait()
        result = await super()._next_frame()
        await self._gate.wait()
        return result

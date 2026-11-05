from abc import ABC, abstractmethod
from asyncio import (
    FIRST_COMPLETED,
    Condition,
    Event,
    Future,
    wait,
)
from asyncio import (
    create_task as asyncio_create_task,
)
from collections.abc import Callable, Coroutine
from contextvars import copy_context
from typing import Any, overload

from .base import BaseDecoupledStage, BaseStage, ElementState

type CreateTaskFunc[Sink_co, Source_co] = Callable[
    [Sink_co], Coroutine[None, None, Source_co]
]


class BaseCreateTask[Sink_co, Source_co](BaseStage[Sink_co, Future[Source_co]], ABC):
    """
    Abstract stage that schedules a coroutine for each input frame.

    A subclass implements ``_create_coroutine(frame)`` to build the coroutine. The
    stage schedules it on the pipeline's event loop with a copy of the current
    context and emits the task without waiting for its result. :class:`CreateTask`
    accepts a coroutine builder function instead of requiring a subclass.

    Input and output::

        input:  1, 2, 3
        output: Task[str], Task[str], Task[str]

    Usage:

    .. code-block:: python

        from collections.abc import Coroutine

        from pyrill.futures import BaseCreateTask
        from pyrill.sources import IterableSource

        class Stringify(BaseCreateTask[int, str]):
            async def convert(self, frame: int) -> str:
                return str(frame)

            def _create_coroutine(self, frame: int) -> Coroutine[None, None, str]:
                return self.convert(frame)

        source = IterableSource(source=[1, 2, 3])
        stage = source >> Stringify()

        await stage.mount()
        tasks = [task async for task in stage]
        results = [await task for task in tasks]
        # ["1", "2", "3"]
    """

    async def _process_frame(self, frame: Any) -> Future[Source_co]:
        return self.pipeline.loop.create_task(
            self._create_coroutine(frame), context=copy_context()
        )

    @abstractmethod
    def _create_coroutine(
        self, frame: Sink_co
    ) -> Coroutine[None, None, Source_co]:  # pragma: nocover
        raise NotImplementedError()


class CreateTask[Sink_co, Source_co](BaseCreateTask[Sink_co, Source_co]):
    """
    Stage that creates an asynchronous task for each frame using a function.

    The function receives the input frame and returns a coroutine. Each coroutine
    runs on the pipeline's event loop with a copy of the current context. Output
    tasks follow input order; their results may become available in a different
    order. Await the tasks to receive their results or exceptions, or connect an
    :class:`AsCompleted` stage to consume results as tasks finish.

    :param coroutine_builder_func: Function that builds a coroutine for each frame.
        An ``async def`` function works directly.

    Input and output::

        input:  1, 2, 3
        output: Task[str], Task[str], Task[str]

    Usage:

    .. code-block:: python

        from pyrill.futures import CreateTask
        from pyrill.sources import IterableSource

        async def stringify(frame: int) -> str:
            return str(frame)

        source = IterableSource(source=[1, 2, 3])
        stage = source >> CreateTask(coroutine_builder_func=stringify)

        await stage.mount()
        tasks = [task async for task in stage]
        results = [await task for task in tasks]
        # ["1", "2", "3"]
    """

    def __init__(
        self,
        *args: Any,
        coroutine_builder_func: CreateTaskFunc[Sink_co, Source_co],
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._coroutine_builder_func = coroutine_builder_func

    def _create_coroutine(self, frame: Sink_co) -> Coroutine[None, None, Source_co]:
        return self._coroutine_builder_func(frame)


@overload
def create_task[Sink_co, Source_co](
    name: str | None,
    func: CreateTaskFunc[Sink_co, Source_co],
) -> CreateTask[Sink_co, Source_co]: ...


@overload
def create_task[Sink_co, Source_co](
    name: str | None = None,
    func: None = None,
) -> Callable[[CreateTaskFunc[Sink_co, Source_co]], CreateTask[Sink_co, Source_co]]: ...


def create_task[Sink_co, Source_co](
    name: str | None = None,
    func: CreateTaskFunc[Sink_co, Source_co] | None = None,
) -> (
    CreateTask[Sink_co, Source_co]
    | Callable[[CreateTaskFunc[Sink_co, Source_co]], CreateTask[Sink_co, Source_co]]
):
    """
    Create a :class:`CreateTask` stage from a coroutine function.

    Call ``create_task(name, func)`` directly or use ``@create_task()`` or
    ``@create_task("name")`` as a decorator. The decorated name becomes a stage
    that emits tasks rather than the original function. See :class:`CreateTask`
    for how each task is scheduled and its result is consumed.

    :param name: Stage name. If omitted or empty, use the function's name.
    :param func: Function that receives a frame and returns a coroutine. If
        omitted, return a decorator that accepts the function.
    :return: A stage when ``func`` is supplied, otherwise a stage decorator.

    Usage:

    .. code-block:: python

        from pyrill.futures import create_task
        from pyrill.sources import IterableSource

        @create_task("stringify")
        async def stringify(frame: int) -> str:
            return str(frame)

        source = IterableSource(source=[1, 2, 3])
        stage = source >> stringify

        await stage.mount()
        tasks = [task async for task in stage]
        results = [await task for task in tasks]
        # ["1", "2", "3"]
    """

    def inner(
        func: CreateTaskFunc[Sink_co, Source_co],
    ) -> CreateTask[Sink_co, Source_co]:
        return CreateTask(name=name or func.__name__, coroutine_builder_func=func)

    return inner(func) if func is not None else inner


class AsCompleted[Source_co](BaseDecoupledStage[Future[Source_co], Source_co]):
    """
    Stage that emits future results as they become available.

    A background task reads futures from upstream while the downstream waits for
    completed futures. Each output is the result of awaiting a completed future,
    so its exception is propagated to the reader. Input order is not preserved;
    when several futures are already complete, their output order is unspecified.
    Unmounting cancels unfinished futures held in the pending buffer.

    :param max_pending: Maximum number of buffered futures, including completed
        futures whose results have not yet been emitted. ``None`` means unlimited,
        the default. Use a positive integer to make the upstream wait while the
        buffer is full.

    Input and output::

        input:  Future[int](1), Future[int](2), Future[int](3)
        output: 2, 3, 1 (if the futures finish in that order)

    Usage:

    .. code-block:: python

        from pyrill.futures import AsCompleted, CreateTask
        from pyrill.sources import IterableSource

        async def double(frame: int) -> int:
            return frame * 2

        source = IterableSource(source=[1, 2, 3])
        stage = (
            source
            >> CreateTask(coroutine_builder_func=double)
            >> AsCompleted(max_pending=2)
        )

        await stage.mount()
        results = [result async for result in stage]
        # Contains 2, 4, 6; order depends on task completion.
    """

    _pending: set[Future[Source_co]]
    _done: set[Future[Source_co]]
    _pushed_exception: BaseException | None = None

    def __init__(self, *args: Any, max_pending: int | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._max_pending = max_pending
        self._event = Event()
        self._condition = Condition()

    async def _mount(self) -> None:
        self._pending = set()
        self._done = set()
        self._event.clear()
        async with self._condition:
            self._condition.notify_all()
        return await super()._mount()

    async def _unmount(self) -> None:
        [fut.cancel() for fut in self._pending if not fut.done()]
        self._pending = set()
        self._done = set()
        self._event.clear()
        return await super()._unmount()

    async def _push_frame(self, frame: Future[Source_co] | BaseException) -> None:
        if self._pushed_exception is not None:
            raise RuntimeError("An exception already pushed")

        async with self._condition:
            if isinstance(frame, BaseException):
                self._pushed_exception = frame
                self._condition.notify()
                return

            if self._max_pending is not None:
                await self._condition.wait_for(
                    lambda: (
                        self._max_pending is None
                        or len(self._done) + len(self._pending) < self._max_pending
                        or self.state != ElementState.READY
                    )
                )
                if self.state != ElementState.READY:
                    return

        self._pending.add(frame)
        self._event.set()

        async with self._condition:
            self._condition.notify()

    async def _decoupled_next_frame(self) -> Future[Source_co]:
        while True:
            try:
                result = self._done.pop()
            except KeyError:
                async with self._condition:
                    await self._condition.wait_for(
                        lambda: len(self._pending) > 0 or self._pushed_exception
                    )
                    if len(self._pending) == 0 and self._pushed_exception:
                        raise self._pushed_exception

                    wait_fut = asyncio_create_task(
                        wait(self._pending, return_when=FIRST_COMPLETED)
                    )
                    self._event.clear()

                event_fut = asyncio_create_task(self._event.wait())
                await wait((wait_fut, event_fut), return_when=FIRST_COMPLETED)

                if wait_fut.done():
                    self._done, self._pending = await wait_fut
            else:
                async with self._condition:
                    self._condition.notify()
                return result

    async def _process_frame(self, frame: Future[Source_co]) -> Source_co:
        return await frame

from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
)
from typing import Any

from .base import BaseSource, ElementState

__all__ = ["AsyncIterableSource", "IterableSource"]


class IterableSource[Source_co](BaseSource[Source_co]):
    """
    Source that emits the items of a synchronous iterable.

    The iterable is read from its start every time the source is mounted, so a list
    can be replayed after a remount. The stream ends when the iterable is exhausted.

    :param source: Iterable (list, tuple, generator, ...) to read frames from.

    Input and output::

        source: ["a", "b"]
        output: "a", "b"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b"])

        await source.mount()
        frames = [frame async for frame in source]
        # ["a", "b"]
    """

    _source: Iterable[Source_co]
    _iter: Iterator[Source_co] | None = None

    def __init__(self, *args: Any, source: Iterable[Source_co], **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._source = source

    @property
    def source(self) -> Iterable[Source_co]:
        """
        Iterable that supplies the frames.
        """
        return self._source

    async def _mount(self) -> None:
        self._iter = iter(self._source)
        await super()._mount()

    async def _unmount(self) -> None:
        self._iter = None

        await super()._unmount()

    async def _next_frame(self) -> Source_co:
        if self.state is not ElementState.READY:
            raise RuntimeError("Not ready")
        if self._iter is None:
            raise RuntimeError("Iterator has not been initiated")

        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration()


class AsyncIterableSource[Source_co](BaseSource[Source_co]):
    """
    Source that emits the items of an asynchronous iterable.

    It calls ``aiter`` on the object each time it is mounted. The stream ends when the
    async iterable raises ``StopAsyncIteration``; other exceptions put the source in
    ``ERROR``.

    :param source: Async iterable (async generator, ...) to read frames from.

    Input and output::

        source: async generator yielding 1, 2, 3
        output: 1, 2, 3

    Usage:

    .. code-block:: python

        from collections.abc import AsyncIterator

        from pyrill.sources import AsyncIterableSource


        async def numbers() -> AsyncIterator[int]:
            for number in (1, 2, 3):
                yield number


        source = AsyncIterableSource(source=numbers())

        await source.mount()
        frames = [frame async for frame in source]
        # [1, 2, 3]
    """

    _source: AsyncIterable[Source_co]
    _iter: AsyncIterator[Source_co] | None = None

    def __init__(self, *args: Any, source: AsyncIterable[Source_co], **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._source = source

    @property
    def source(self) -> AsyncIterable[Source_co]:
        """
        Async iterable that supplies the frames.
        """
        return self._source

    async def _mount(self) -> None:
        self._iter = aiter(self._source)
        await super()._mount()

    async def _unmount(self) -> None:
        self._iter = None

        await super()._unmount()

    async def _next_frame(self) -> Source_co:
        if self.state is not ElementState.READY:
            raise RuntimeError("Not ready")
        if self._iter is None:
            raise RuntimeError("Iterator has not been initiated")

        return await anext(self._iter)


class BuilderSource[Source_co](BaseSource[Source_co]):
    """
    Source that produces each frame by awaiting a builder function.

    The builder is called with no arguments once per frame and never ends the stream by
    itself. Raise ``StopAsyncIteration`` from it to finish, or raise
    :class:`pyrill.base.FrameSkipped` to drop a frame. The :func:`builder` function
    creates one from a coroutine function.

    :param builder: Coroutine function without parameters that returns the next frame.

    Input and output::

        builder: returns 1, 2, 3, then raises StopAsyncIteration
        output:  1, 2, 3

    Usage:

    .. code-block:: python

        from pyrill.sources import BuilderSource

        numbers = iter([1, 2, 3])


        async def build() -> int:
            try:
                return next(numbers)
            except StopIteration:
                raise StopAsyncIteration()


        source = BuilderSource(builder=build)

        await source.mount()
        frames = [frame async for frame in source]
        # [1, 2, 3]
    """

    _builder: Callable[..., Awaitable[Source_co]]

    def __init__(
        self, *args: Any, builder: Callable[..., Awaitable[Source_co]], **kwargs: Any
    ):
        super().__init__(*args, **kwargs)

        self._builder = builder

    async def _next_frame(self) -> Source_co:
        return await self._builder()


def builder[Source_co](
    func: Callable[..., Awaitable[Source_co]],
) -> BuilderSource[Source_co]:
    """
    Create a :class:`BuilderSource` from a coroutine function.

    It works as a function call or as a decorator; the decorated name becomes the source
    and not the function. See :class:`BuilderSource` for how the function ends the stream.

    :param func: Coroutine function without parameters that returns the next frame.

    Usage:

    .. code-block:: python

        from pyrill.sources import builder

        numbers = iter([1, 2])


        @builder
        async def source() -> int:
            try:
                return next(numbers)
            except StopIteration:
                raise StopAsyncIteration()


        await source.mount()
        frames = [frame async for frame in source]
        # [1, 2]
    """
    return BuilderSource(builder=func)

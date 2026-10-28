from abc import abstractmethod
from collections.abc import Awaitable, Callable
from inspect import Parameter, isawaitable, signature
from typing import Any, Concatenate, Protocol, cast, overload

from .base import BaseStage

__all__ = ["BaseMap", "Map", "make_map"]


type MapFunc[Sink_co, Source_co] = Callable[[Sink_co], Awaitable[Source_co] | Source_co]

type MapperFunc[Sink_co, Source_co, **Params] = Callable[
    Concatenate[Sink_co, Params], Awaitable[Source_co] | Source_co
]


class BaseMap[Sink_co, Source_co](BaseStage[Sink_co, Source_co]):
    """
    Abstract stage that transforms each frame with :meth:`_map_frame`.

    The base class calls ``_map_frame(frame)`` for every frame and emits the result.
    The method may be a plain function or a coroutine function: when it returns an
    awaitable, the stage awaits it. A subclass must implement
    ``_map_frame(self, frame)``; :class:`Map` and :func:`make_map` build on it.

    Input and output::

        input:  1, 2, 3
        output: 2, 4, 6

    Usage:

    .. code-block:: python

        from pyrill.mappers import BaseMap
        from pyrill.sources import IterableSource

        class Double(BaseMap[int, int]):
            def _map_frame(self, frame: int) -> int:
                return frame * 2

        source = IterableSource(source=[1, 2, 3])
        stage = source >> Double()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [2, 4, 6]
    """

    @abstractmethod
    def _map_frame(
        self, frame: Sink_co
    ) -> Source_co | Awaitable[Source_co]:  # pragma: nocover
        raise NotImplementedError()

    async def _process_frame(self, frame: Sink_co) -> Source_co:
        result = self._map_frame(frame)
        if isawaitable(result):
            return await result
        return result


class Map[Sink_co, Source_co](BaseMap[Sink_co, Source_co]):
    """
    Stage that maps each frame with a function.

    The function receives the frame and returns the new frame, either directly or as an
    awaitable (an ``async def`` function works). Use :func:`map` to create a ``Map``
    from a decorated function, or :func:`make_map` when the stage needs options.

    :param map_func: Function applied to every frame. It may be synchronous or
        asynchronous.

    Input and output::

        input:  "a", "b"
        output: "A", "B"

    Usage:

    .. code-block:: python

        from pyrill.mappers import Map
        from pyrill.sources import IterableSource

        source = IterableSource(source=["a", "b"])
        stage = source >> Map(map_func=str.upper)

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["A", "B"]
    """

    map_func: MapFunc[Sink_co, Source_co]

    def __init__(
        self,
        *args: Any,
        map_func: MapFunc[Sink_co, Source_co],
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.map_func = map_func

    def _map_frame(self, frame: Sink_co) -> Source_co | Awaitable[Source_co]:
        return self.map_func(frame)


@overload
def map[Sink_co, Source_co](
    name: str | None,
    func: MapFunc[Sink_co, Source_co],
) -> Map[Sink_co, Source_co]: ...


@overload
def map[Sink_co, Source_co](
    name: str | None = None,
    func: None = None,
) -> Callable[[MapFunc[Sink_co, Source_co]], Map[Sink_co, Source_co]]: ...


def map[Sink_co, Source_co](
    name: str | None = None,
    func: MapFunc[Sink_co, Source_co] | None = None,
) -> (
    Map[Sink_co, Source_co]
    | Callable[[MapFunc[Sink_co, Source_co]], Map[Sink_co, Source_co]]
):
    """
    Create a :class:`Map` stage from a function, directly or as a decorator.

    Called as ``map(name, func)`` it returns a ``Map`` that wraps ``func``. Called
    without ``func`` (``map()`` or ``map("name")``) it returns a decorator that does the
    same; the stage is named ``name``, or the function name when ``name`` is ``None``.
    The decorated name becomes a stage instance, not a function. Note that ``@map``
    without parentheses does not work, because the function would be taken as the name.

    :param name: Name of the stage. Defaults to the name of the function.
    :param func: Function to apply to every frame, synchronous or asynchronous. When
        omitted, a decorator is returned.

    Input and output::

        input:  1, 2, 3
        output: 11, 12, 13

    Usage:

    .. code-block:: python

        from pyrill.mappers import map as map_stage
        from pyrill.sources import IterableSource

        @map_stage()
        def add_ten(frame: int) -> int:
            return frame + 10

        source = IterableSource(source=[1, 2, 3])
        stage = source >> add_ten

        await stage.mount()
        frames = [frame async for frame in stage]
        # [11, 12, 13]
    """

    def inner(func: MapFunc[Sink_co, Source_co]) -> Map[Sink_co, Source_co]:
        return Map(name=name or func.__name__, map_func=func)

    return inner(func) if func is not None else inner


def extract_kwargs(kwargs: dict[str, Any], func: Callable[..., Any]) -> dict[str, Any]:
    """
    Pop from ``kwargs`` the entries named like the keyword-only parameters of ``func``.

    It modifies ``kwargs`` in place and returns the removed entries. The classes built
    by :func:`make_map` use it to split their constructor keywords between the mapped
    function and the stage itself.
    """
    result: dict[str, Any] = {}

    func_sig = signature(func)
    for name, param in func_sig.parameters.items():
        if param.kind != Parameter.KEYWORD_ONLY:
            continue
        try:
            result[name] = kwargs.pop(name)
        except KeyError:
            continue

    return result


class MapCallback[Source_co, Sink_co](Protocol):
    """
    Protocol of a callable that maps a frame, with optional keyword arguments.

    It describes ``f(frame, **kwargs)`` returning the new frame or an awaitable of it,
    and is used for typing only.
    """

    def __call__(
        self, frame: Source_co, **kwargs: Any
    ) -> Sink_co | Awaitable[Sink_co]: ...


# The frame parameter must be positional-only (``def f(frame: T, /, ...)``) for
# mypy to match it against Concatenate. The async overload comes first so that
# mypy infers Source_co from the awaited result rather than the coroutine.
@overload
def make_map[Sink_co, Source_co, **Params](
    func: Callable[Concatenate[Sink_co, Params], Awaitable[Source_co]],
) -> type[BaseMap[Sink_co, Source_co]]: ...


@overload
def make_map[Sink_co, Source_co, **Params](
    func: Callable[Concatenate[Sink_co, Params], Source_co],
) -> type[BaseMap[Sink_co, Source_co]]: ...


def make_map[Sink_co, Source_co, **Params](
    func: MapperFunc[Sink_co, Source_co, Params],
) -> type[BaseMap[Sink_co, Source_co]]:
    """
    Turn a function into a stage class (decorator).

    The function must be ``def f(frame, /, *, option=...)``: the frame parameter is
    positional-only and every keyword-only parameter becomes a constructor keyword of
    the new class, which is passed to the function on every call. The function may be
    synchronous or asynchronous. The decorated name becomes a :class:`BaseMap` subclass
    that keeps the name, module and docstring of the function, so it is instantiated
    before use. The remaining constructor arguments (for example ``name``) go to the
    stage.

    Input and output::

        input:  1, 2, 3
        output: 3, 6, 9    (with ``factor=3``)

    Usage:

    .. code-block:: python

        from pyrill.mappers import make_map
        from pyrill.sources import IterableSource

        @make_map
        def scale(frame: int, /, *, factor: int = 2) -> int:
            return frame * factor

        source = IterableSource(source=[1, 2, 3])
        stage = source >> scale(factor=3)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [3, 6, 9]
    """

    class Mapper(BaseMap[Sink_co, Source_co]):
        def __init__(self, *args: Any, **kwargs: Any):
            self._kwargs: dict[str, Any] = extract_kwargs(kwargs, func)

            super().__init__(*args, **kwargs)

        def _map_frame(self, frame: Sink_co) -> Source_co | Awaitable[Source_co]:
            return cast(Callable[..., Awaitable[Source_co] | Source_co], func)(
                frame, **self._kwargs
            )

    Mapper.__doc__ = func.__doc__
    Mapper.__module__ = func.__module__
    Mapper.__name__ = func.__name__
    Mapper.__qualname__ = func.__qualname__
    return Mapper

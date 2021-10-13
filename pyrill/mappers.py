from inspect import Parameter, isawaitable, signature
from typing import Any, Awaitable, Callable, Dict, Union

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

from .base import BaseStage, FrameSkippedError, Sink_co, Source_co

__all__ = ['BaseMap', 'Map', 'make_map']


class BaseMap(BaseStage[Sink_co, Source_co]):

    def __init__(self, *args, skip_errors=False, **kwargs):
        super(BaseMap, self).__init__(*args, **kwargs)

        self._skip_errors = skip_errors

    def _map_func(self, frame: Source_co) -> Sink_co:  # pragma: nocover
        raise NotImplementedError()

    async def process_frame(self, frame: Source_co) -> Sink_co:
        try:
            result = self._map_func(frame)
            if isawaitable(result):
                result = await result
        except StopAsyncIteration:
            raise
        except Exception:
            if self._skip_errors:
                raise FrameSkippedError(frame)
            raise

        return result


class Map(BaseMap[Sink_co, Source_co]):
    map_func: Callable[[Sink_co], Union[Awaitable[Source_co], Source_co]]

    def __init__(self, *args, map_func: Callable[[Sink_co], Union[Awaitable[Source_co], Source_co]], **kwargs):
        super(Map, self).__init__(*args, **kwargs)

        self.map_func = map_func

    def _map_func(self, frame: Source_co) -> Sink_co:
        return self.map_func(frame)


def extract_kwargs(kwargs: Dict, func: Callable) -> Dict:
    result = {}

    func = signature(func)
    for name, param in func.parameters.items():
        if param.kind != Parameter.KEYWORD_ONLY:
            continue
        try:
            result[name] = kwargs.pop(name)
        except KeyError:
            continue

    return result


class MapCallback(Protocol[Source_co, Sink_co]):  # pragma: nocover
    def __call__(self, frame: Source_co, **kwargs: Any) -> Sink_co:
        pass


def make_map(func: MapCallback[Source_co, Sink_co]):
    class Mapper(BaseMap[Source_co, Sink_co]):

        def __init__(self, *args, **kwargs):
            self._kwargs = extract_kwargs(kwargs, func)

            super(Mapper, self).__init__(*args, **kwargs)

        def _map_func(self, frame: Source_co) -> Sink_co:
            return func(frame, **self._kwargs)

    Mapper.__doc__ = func.__doc__
    Mapper.__module__ = func.__module__
    Mapper.__name__ = func.__name__
    Mapper.__qualname__ = func.__qualname__
    return Mapper

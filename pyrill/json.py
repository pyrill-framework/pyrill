from inspect import isawaitable
from json import dumps, loads
from typing import Any, Dict, Mapping, Optional, Tuple, cast

from .base import BaseElement, BaseProducer, BaseSource, BaseStage
from .primitives import BaseBinStage, PrefixStream, SuffixStream
from .sources import SyncSource

__all__ = ['BaseFromJson', 'BaseToJson', 'FromJson', 'ToJson', 'ToJsonList',
           'ToJsonObject', 'DataToJson', 'ToJsonPerLine']


class BaseFromJson(BaseElement):
    def __init__(self,
                 *args,
                 json_decoder_cls=None,
                 json_decoder_kwargs: Dict = None,
                 **kwargs):
        super(BaseFromJson, self).__init__(*args, **kwargs)

        self.json_decoder_cls = json_decoder_cls

        self.json_decoder_kwargs = json_decoder_kwargs or {}

    def from_json(self, data: str) -> Any:
        return loads(data, cls=self.json_decoder_cls, **self.json_decoder_kwargs)


class FromJson(BaseFromJson, BaseStage[str, Any]):

    async def process_frame(self, frame: Any) -> str:
        return self.from_json(frame)


class BaseToJson(BaseProducer[str]):

    def __init__(self, *args, json_encoder_cls=None, json_encoder_kwargs: Dict = None, **kwargs):
        super(BaseToJson, self).__init__(*args, **kwargs)

        self.json_encoder_cls = json_encoder_cls

        self.json_encoder_kwargs = json_encoder_kwargs or {}

        self.json_encoder_kwargs.setdefault('separators', (', ', ': '))
        self.json_encoder_kwargs.setdefault('indent', None)

    def get_key_separator(self) -> str:
        return self.json_encoder_kwargs['separators'][1]

    def get_frame_separator(self) -> str:
        return self.json_encoder_kwargs['separators'][0]

    def to_json(self, data: Any) -> str:
        return dumps(data, cls=self.json_encoder_cls, **self.json_encoder_kwargs)


class ToJson(BaseToJson, BaseStage[Any, str]):
    async def process_frame(self, frame: Any) -> str:
        return self.to_json(frame)


class ToJsonPerLine(ToJson):

    def __init__(self, *args, **kwargs):
        super(ToJsonPerLine, self).__init__(*args, **kwargs)

        self.json_encoder_kwargs['separators'] = (',', ':')
        self.json_encoder_kwargs['indent'] = None

    async def process_frame(self, frame) -> str:
        return await super(ToJsonPerLine, self).process_frame(frame) + '\n'


class DataToJson(BaseToJson, BaseSource[str]):

    def __init__(self, *args, data: Any, **kwargs):
        super(DataToJson, self).__init__(*args, **kwargs)

        self._data: Any = data
        self._eos = False

    async def _mount(self):
        self._eos = False

        await super(DataToJson, self)._mount()

    async def _next_frame(self) -> str:
        if self._eos:
            raise StopAsyncIteration()
        result = self.to_json(self._data)
        self._eos = True
        return result


def _build_json_iter_from(value, json_encoder_cls=None, json_encoder_kwargs: Dict = None):
    if isinstance(value, BaseToJson):
        return value
    elif isinstance(value, (dict, Mapping)):
        return SyncSource(source=[(str(k), v) for k, v in value.items()]) \
            >> ToJsonObject(json_encoder_cls=json_encoder_cls,
                            json_encoder_kwargs=json_encoder_kwargs)
    elif isinstance(value, (list, tuple, set)):
        return SyncSource(source=value) \
            >> ToJsonList(json_encoder_cls=json_encoder_cls,
                          json_encoder_kwargs=json_encoder_kwargs)

    return DataToJson(data=value)


class ToJsonInnerList(BaseToJson, BaseStage[Any, str]):
    _iter_value: Optional[BaseProducer[str]] = None

    async def _mount(self):
        self._iter_value = None
        self._first = True

        await super(ToJsonInnerList, self)._mount()

    async def _next_frame(self) -> str:
        while True:
            try:
                if self._iter_value:
                    return await self._iter_value.__anext__()
            except StopAsyncIteration:
                pass

            value = await super(ToJsonInnerList, self)._next_frame()

            self._iter_value = _build_json_iter_from(value,
                                                     json_encoder_cls=self.json_encoder_cls,
                                                     json_encoder_kwargs=self.json_encoder_kwargs.copy())
            if self._first:
                self._first = False
            else:
                return self.get_frame_separator()

    async def process_frame(self, frame: Any) -> Any:
        if isawaitable(frame):
            frame = await frame
        return frame


class ToJsonList(BaseToJson, BaseBinStage[Any, str]):

    def __init__(self, *args, **kwargs):
        super(ToJsonList, self).__init__(*args, **kwargs)

        upstream_elem = ToJsonInnerList(json_encoder_cls=self.json_encoder_cls,
                                        json_encoder_kwargs=self.json_encoder_kwargs)
        self.set_upstream_consumer(upstream_elem)

        downstream_elem = upstream_elem \
            >> PrefixStream(prefix='[') \
            >> SuffixStream(suffix=']')

        self.set_downstream_producer(downstream_elem)


class ToJsonInnerObject(BaseToJson, BaseStage[Tuple[str, Any], str]):
    _iter_value: Optional[BaseProducer[str]] = None

    async def _mount(self):
        self._iter_value = None
        self._first = True

        await super(ToJsonInnerObject, self)._mount()

    async def _next_frame(self) -> str:
        while True:
            try:
                if self._iter_value:
                    return await self._iter_value.__anext__()
            except StopAsyncIteration:
                pass

            key, value = cast(Tuple[str, Any], await super(ToJsonInnerObject, self)._next_frame())

            if isawaitable(value):
                value = await value

            self._iter_value = _build_json_iter_from(value,
                                                     json_encoder_cls=self.json_encoder_cls,
                                                     json_encoder_kwargs=self.json_encoder_kwargs.copy())

            key_str = self.to_json(str(key)) + self.get_key_separator()

            if self._first:
                self._first = False
                return key_str
            else:
                return self.get_frame_separator() + key_str

    async def process_frame(self, frame: Any) -> Any:
        return frame


class ToJsonObject(BaseToJson, BaseBinStage[Tuple[str, Any], str]):

    def __init__(self, *args, **kwargs):
        super(ToJsonObject, self).__init__(*args, **kwargs)

        upstream_elem = ToJsonInnerObject(json_encoder_cls=self.json_encoder_cls,
                                          json_encoder_kwargs=self.json_encoder_kwargs)
        self.set_upstream_consumer(upstream_elem)

        downstream_elem = upstream_elem \
            >> PrefixStream(prefix='{') \
            >> SuffixStream(suffix='}')

        self.set_downstream_producer(downstream_elem)

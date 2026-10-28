from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import suppress
from json import JSONDecoder, JSONEncoder, dumps, loads
from typing import Any, cast

from .base import BaseConsumer, BaseProducer, BaseSource, BaseStage, Pipeline
from .primitives import BaseBin, PrefixStream, SuffixStream
from .sources import IterableSource

__all__ = [
    "DataToJson",
    "FromJson",
    "ToJson",
    "ToJsonList",
    "ToJsonMixin",
    "ToJsonObject",
    "ToJsonPerLine",
]


type JSONValue = (
    str | int | float | bool | None | dict[str, JSONValue] | list[JSONValue]
)
type JsonableValue = (
    str
    | int
    | float
    | bool
    | None
    | dict[str, JsonableValue]
    | list[JsonableValue]
    | tuple[JsonableValue, ...]
    | set[JsonableValue]
    | ToJson
    | ToJsonList
    | ToJsonObject
)


class FromJsonMixin:
    """
    Mixin that gives a stage the ability to decode JSON text.

    It adds the ``json_decoder_cls`` and ``json_decoder_kwargs`` options and the
    :meth:`from_json` method. The mixin has no abstract methods: a stage class lists it
    before its stage base class and calls ``self.from_json(text)`` from its own
    ``_process_frame``. :class:`FromJson` is the plain implementation.

    :param json_decoder_cls: Custom :class:`json.JSONDecoder` subclass. Defaults to
        the standard decoder.
    :param json_decoder_kwargs: Extra keyword arguments for :func:`json.loads`, for
        example ``{"parse_float": str}``. Defaults to none.

    Input and output::

        input:  '{"id": 7, "tags": []}', '{"id": 8, "tags": []}'
        output: 7, 8

    Usage:

    .. code-block:: python

        from typing import Any

        from pyrill.base import BaseStage
        from pyrill.json import FromJsonMixin
        from pyrill.sources import IterableSource


        class IdFromJson(FromJsonMixin, BaseStage[str, Any]):
            async def _process_frame(self, frame: str) -> Any:
                return self.from_json(frame)["id"]


        source = IterableSource(source=['{"id": 7, "tags": []}', '{"id": 8, "tags": []}'])
        stage = source >> IdFromJson()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [7, 8]
    """

    def __init__(
        self,
        *args: Any,
        json_decoder_cls: type[JSONDecoder] | None = None,
        json_decoder_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.json_decoder_cls: type[JSONDecoder] | None = json_decoder_cls

        self.json_decoder_kwargs: dict[str, Any] = json_decoder_kwargs or {}

    def from_json(self, data: str) -> JSONValue:
        """
        Decode a JSON text with the configured decoder and return the Python value.

        Invalid JSON raises :class:`json.JSONDecodeError`.
        """

        return cast(
            JSONValue,
            loads(data, cls=self.json_decoder_cls, **self.json_decoder_kwargs),
        )


class FromJson(FromJsonMixin, BaseStage[str, JSONValue]):
    """
    Stage that decodes each JSON text frame into a Python value.

    Each frame must hold one complete JSON document. Objects become dicts, arrays
    become lists, and so on. An invalid document raises :class:`json.JSONDecodeError`
    and puts the element in the error state. See :class:`FromJsonMixin` for the
    decoder options.

    :param json_decoder_cls: Custom :class:`json.JSONDecoder` subclass. Optional.
    :param json_decoder_kwargs: Extra keyword arguments for :func:`json.loads`.

    Input and output::

        input:  '{"a": 1}', '[1, 2]', '3'
        output: {"a": 1}, [1, 2], 3

    Usage:

    .. code-block:: python

        from pyrill.json import FromJson
        from pyrill.sources import IterableSource

        source = IterableSource(source=['{"a": 1}', "[1, 2]", "3"])
        stage = source >> FromJson()

        await stage.mount()
        frames = [frame async for frame in stage]
        # [{"a": 1}, [1, 2], 3]
    """

    async def _process_frame(self, frame: str) -> JSONValue:
        return self.from_json(frame)


class ToJsonMixin:
    """
    Mixin that gives an element the ability to encode Python values as JSON text.

    It adds the ``json_encoder_cls`` and ``json_encoder_kwargs`` options and the
    :meth:`to_json`, :meth:`get_key_separator` and :meth:`get_frame_separator`
    methods. By default the separators are ``", "`` between items and ``": "`` after
    keys, and there is no indentation; these are set with ``setdefault`` so the caller
    can override them. The mixin has no abstract methods: an element class lists it
    before its base class and calls ``self.to_json(value)`` where needed.
    :class:`ToJson` and :class:`DataToJson` are plain implementations, and the list and
    object encoders use the separators to build their fragments.

    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`, for
        example ``{"sort_keys": True}``. Defaults to none.

    Input and output::

        input:  {"b": 1, "a": 2}
        output: '{"a":2,"b":1}'

    Usage:

    .. code-block:: python

        from typing import Any

        from pyrill.base import BaseStage
        from pyrill.json import ToJsonMixin
        from pyrill.sources import IterableSource


        class SortedToJson(ToJsonMixin, BaseStage[Any, str]):
            async def _process_frame(self, frame: Any) -> str:
                return self.to_json(frame)


        source = IterableSource(source=[{"b": 1, "a": 2}])
        stage = source >> SortedToJson(
            json_encoder_kwargs={"sort_keys": True, "separators": (",", ":")}
        )

        await stage.mount()
        frames = [frame async for frame in stage]
        # ['{"a":2,"b":1}']
    """

    def __init__(
        self,
        *args: Any,
        json_encoder_cls: type[JSONEncoder] | None = None,
        json_encoder_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.json_encoder_cls = json_encoder_cls

        self.json_encoder_kwargs = json_encoder_kwargs or {}

        self.json_encoder_kwargs.setdefault("separators", (", ", ": "))
        self.json_encoder_kwargs.setdefault("indent", None)

    def _separators(self) -> tuple[str, str]:
        return cast(tuple[str, str], self.json_encoder_kwargs["separators"])

    def get_key_separator(self) -> str:
        """
        Return the text written between an object key and its value, ``": "`` by default.
        """

        return self._separators()[1]

    def get_frame_separator(self) -> str:
        """
        Return the text written between two items of a container, ``", "`` by default.
        """

        return self._separators()[0]

    def to_json(self, data: JSONValue | JsonableValue) -> str:
        """
        Encode a value as JSON text with the configured encoder and options.
        """

        return dumps(data, cls=self.json_encoder_cls, **self.json_encoder_kwargs)


class ToJson(ToJsonMixin, BaseStage[JSONValue, str]):
    """
    Stage that encodes each frame as one JSON text frame.

    Each incoming value is encoded as a whole, so one frame produces one string. For
    large or nested values that should be emitted in pieces, use :class:`ToJsonList` or
    :class:`ToJsonObject`. A value that cannot be encoded raises :class:`TypeError`
    and puts the element in the error state. See :class:`ToJsonMixin` for the
    encoder options.

    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`, for
        example ``{"sort_keys": True}``. Defaults to none.

    Input and output::

        input:  {"a": [1, 2]}, 3
        output: '{"a": [1, 2]}', '3'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJson
        from pyrill.sources import IterableSource

        source = IterableSource(source=[{"a": [1, 2]}, 3])
        stage = source >> ToJson()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ['{"a": [1, 2]}', '3']
    """

    async def _process_frame(self, frame: JSONValue) -> str:
        return self.to_json(frame)


class ToJsonPerLine(ToJson):
    """
    Stage that encodes each frame as one compact JSON line (JSON Lines).

    It works like :class:`ToJson`, but it forces compact separators (``","`` and
    ``":"``) and no indentation, and it appends a newline to every frame. The result
    can be written directly to a file with one document per line.

    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`. The
        ``separators`` and ``indent`` options are always overridden.

    Input and output::

        input:  {"a": [1, 2]}, 3
        output: '{"a":[1,2]}\\n', '3\\n'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJsonPerLine
        from pyrill.sources import IterableSource

        source = IterableSource(source=[{"a": [1, 2]}, 3])
        stage = source >> ToJsonPerLine()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ['{"a":[1,2]}\\n', '3\\n']
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.json_encoder_kwargs["separators"] = (",", ":")
        self.json_encoder_kwargs["indent"] = None

    async def _process_frame(self, frame: JSONValue) -> str:
        return await super()._process_frame(frame) + "\n"


class DataToJson(ToJsonMixin, BaseSource[str]):
    """
    Source that encodes one value as JSON text and then ends.

    The value is given at construction time and emitted as a single string frame.
    Remounting the source emits it again. Unlike :class:`ToJson`, the value may
    contain tuples, sets and :class:`ToJson`, :class:`ToJsonList` or
    :class:`ToJsonObject` stages (see ``JsonableValue``), which are used by the list
    and object encoders for nested values.

    :param data: The value to encode.
    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`.

    Input and output::

        input:  (none, the data is given as a parameter)
        output: '{"a": [1, 2]}'

    Usage:

    .. code-block:: python

        from pyrill.json import DataToJson

        source = DataToJson(data={"a": [1, 2]})

        await source.mount()
        frames = [frame async for frame in source]
        # ['{"a": [1, 2]}']
    """

    def __init__(self, *args: Any, data: JsonableValue, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self._data: JsonableValue = data
        self._eos = False

    async def _mount(self) -> None:
        self._eos = False

        await super()._mount()

    async def _next_frame(self) -> str:
        if self._eos:
            raise StopAsyncIteration()
        result = self.to_json(self._data)
        self._eos = True
        return result


def _build_json_iter_from(
    value: JsonableValue,
    *,
    pipeline: Pipeline,
    json_encoder_cls: type[JSONEncoder] | None = None,
    json_encoder_kwargs: dict[str, Any] | None = None,
) -> BaseProducer[str]:
    if isinstance(value, (ToJson, ToJsonList, ToJsonObject)):
        # Already a stream of JSON fragments: flatten it in place.
        return value

    encoder_kwargs: dict[str, Any] = {
        "json_encoder_cls": json_encoder_cls,
        "json_encoder_kwargs": json_encoder_kwargs,
        "pipeline": pipeline,
    }
    if isinstance(value, Mapping):
        items = [(str(k), v) for k, v in value.items()]
        return IterableSource(source=items, pipeline=pipeline) >> ToJsonObject(
            **encoder_kwargs
        )
    if isinstance(value, (list, tuple, set)):
        return IterableSource(source=value, pipeline=pipeline) >> ToJsonList(
            **encoder_kwargs
        )
    return DataToJson(data=value, **encoder_kwargs)


class _BaseToJsonInner[Sink_co](
    ToJsonMixin, BaseConsumer[Sink_co], BaseProducer[str], ABC
):
    """
    Emits the JSON fragments of a container's items, without the brackets.

    Each consumed frame opens one item: :meth:`_open_item` returns the text that
    precedes its value (e.g. an object key) and the value, which is then streamed
    through a nested producer, so nested containers are encoded incrementally.
    """

    _iter_value: BaseProducer[str] | None = None
    _first: bool = True

    @abstractmethod
    def _open_item(self, frame: Sink_co) -> tuple[str, JsonableValue]:
        raise NotImplementedError()

    async def _mount(self) -> None:
        self._iter_value = None
        self._first = True

        await super()._mount()

    async def _unmount(self) -> None:
        if self._iter_value is not None:
            with suppress(Exception):
                await self._iter_value.unmount()
            self._iter_value = None

        await super()._unmount()

    async def _next_frame(self) -> str:
        while True:
            if self._iter_value is not None:
                try:
                    return await self._iter_value.__anext__()
                except StopAsyncIteration:
                    self._iter_value = None

            head, value = self._open_item(await self.consume_frame())

            self._iter_value = _build_json_iter_from(
                value,
                pipeline=self.pipeline,
                json_encoder_cls=self.json_encoder_cls,
                json_encoder_kwargs=self.json_encoder_kwargs.copy(),
            )
            await self._iter_value.mount()

            if self._first:
                self._first = False
            else:
                head = self.get_frame_separator() + head
            if head:
                return head


class ToJsonInnerList(_BaseToJsonInner[JsonableValue]):
    """
    Internal helper of :class:`ToJsonList` that emits the items of a list as JSON
    fragments, without the brackets. Each item is streamed through a nested encoder
    and consecutive items are separated by the frame separator.
    """

    def _open_item(self, frame: JsonableValue) -> tuple[str, JsonableValue]:
        return "", frame


class ToJsonInnerObject(_BaseToJsonInner[tuple[str, JsonableValue]]):
    """
    Internal helper of :class:`ToJsonObject` that emits ``(key, value)`` pairs as JSON
    fragments, without the braces. Each key is followed by the key separator and each
    value is streamed through a nested encoder.
    """

    def _open_item(self, frame: tuple[str, JsonableValue]) -> tuple[str, JsonableValue]:
        key, value = frame
        return self.to_json(str(key)) + self.get_key_separator(), value


class ToJsonList(ToJsonMixin, BaseBin[JsonableValue, str]):
    """
    Stage that encodes a stream of values as one JSON array, fragment by fragment.

    Concatenating the emitted frames gives the JSON text; nested lists, tuples,
    sets and mappings are streamed the same way.

    Input and output::

        input:  {"a": 1}, [2, 3]
        output: "[", "{", "\"a\": ", "1", "}", ", ", "[", ... "]"
                (joined: '[{"a": 1}, [2, 3]]')
    """

    def _get_upstream_downstream(
        self,
    ) -> tuple[BaseConsumer[JsonableValue], BaseProducer[str]]:
        inner = ToJsonInnerList(
            json_encoder_cls=self.json_encoder_cls,
            json_encoder_kwargs=self.json_encoder_kwargs.copy(),
        )
        return inner, (
            inner >> PrefixStream[str](prefix="[") >> SuffixStream[str](suffix="]")
        )


class ToJsonObject(ToJsonMixin, BaseBin[tuple[str, JsonableValue], str]):
    """
    Stage that encodes a stream of ``(key, value)`` pairs as one JSON object,
    fragment by fragment.

    Concatenating the emitted frames gives the JSON text; nested values are
    streamed the same way as in :class:`ToJsonList`.

    Input and output::

        input:  ("a", 1), ("b", [2])
        output: (joined) '{"a": 1, "b": [2]}'
    """

    def _get_upstream_downstream(
        self,
    ) -> tuple[BaseConsumer[tuple[str, JsonableValue]], BaseProducer[str]]:
        inner = ToJsonInnerObject(
            json_encoder_cls=self.json_encoder_cls,
            json_encoder_kwargs=self.json_encoder_kwargs.copy(),
        )
        return inner, (
            inner >> PrefixStream[str](prefix="{") >> SuffixStream[str](suffix="}")
        )

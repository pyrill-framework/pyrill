from abc import ABC, abstractmethod
from collections.abc import Collection, Iterator, Mapping
from json import JSONDecoder, JSONEncoder, dumps, loads
from typing import Any, cast

from .base import (
    BaseProducer,
    BaseStage,
    FrameSkipped,
)
from .primitives import BaseFlatMap, Flat, SuffixStream
from .sources import IterableSource

__all__ = [
    "FromJson",
    "FromJsonMixin",
    "JSONRawValue",
    "JSONValue",
    "ToJson",
    "ToJsonInnerList",
    "ToJsonInnerObject",
    "ToJsonList",
    "ToJsonObject",
    "ToJsonPerLine",
]


type JSONRawValue = (
    str | int | float | bool | None | Mapping[str, JSONRawValue] | list[JSONRawValue]
)

type JSONValue = (
    JSONRawValue | Mapping[str, JSONValue] | Collection[JSONValue] | BaseToJsonProducer
)


class FromJsonMixin:
    """
    Add configurable JSON decoding to a stage.

    It adds the ``json_decoder_cls`` and ``json_decoder_kwargs`` options and the
    :meth:`from_json` method. A stage class lists this mixin
    before its stage base class and calls ``self.from_json(text)`` from its own
    ``_process_frame``. Each call expects one complete JSON document and raises
    :class:`json.JSONDecodeError` for invalid text. :class:`FromJson` provides a
    stage that returns the decoded value directly.

    :param json_decoder_cls: Custom :class:`json.JSONDecoder` subclass. Defaults to
        the standard decoder.
    :param json_decoder_kwargs: Extra keyword arguments for :func:`json.loads`, for
        example ``{"parse_float": str}``. Defaults to none.

    Input and output::

        input:  '{"id": 7, "tags": []}', '{"id": 8, "tags": []}'
        output: 7, 8

    Usage:

    .. code-block:: python

        from collections.abc import Mapping

        from pyrill.base import BaseStage
        from pyrill.json import FromJsonMixin, JSONValue
        from pyrill.sources import IterableSource


        class IdFromJson(FromJsonMixin, BaseStage[str, JSONValue]):
            async def _process_frame(self, frame: str) -> JSONValue:
                value = self.from_json(frame)
                if not isinstance(value, Mapping):
                    raise TypeError("Expected a JSON object")
                return value["id"]


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
    Decode each complete JSON text frame into one Python value.

    Each frame must hold one complete JSON document. Objects become dicts, arrays
    become lists, and so on. An invalid document raises :class:`json.JSONDecodeError`
    and puts the element in the error state. See :class:`FromJsonMixin` for the
    decoder options. Frames are processed independently: partial documents are
    not buffered across frames, and multiple documents in one frame are rejected.
    An empty input stream produces no output.

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


class BaseToJsonProducer(BaseProducer[str]):
    """
    Base class for producers that emit JSON text fragments.

    It adds the ``depth``, ``json_encoder_cls`` and ``json_encoder_kwargs`` options and the
    :meth:`to_json`, :meth:`get_key_separator` and :meth:`get_frame_separator`
    methods. By default the separators are ``", "`` between items and ``": "`` after
    keys, and there is no indentation; these are set with ``setdefault`` so the caller
    can override them. Subclasses implement production through the inherited
    producer interface. :class:`ToJson`, :class:`ToJsonList` and
    :class:`ToJsonObject` provide concrete serializers. A serializer receiving
    another instance of this base class can forward its fragments directly.

    :meth:`to_json` calls :func:`json.dumps` directly. Recursive dispatch and the
    depth budget are handled by the concrete serializers, not by this method.

    :param depth: Recursive streaming budget. Negative values allow unlimited
        recursion; zero uses direct encoding in :class:`ToJson`. Positive values
        are reduced as container serializers and their inner stages delegate to
        further serializers. Defaults to ``-1``.

    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`, for
        example ``{"sort_keys": True}``. Defaults to none.

    Input and output::

        input:  {"b": 1, "a": 2}
        output: '{"a":2,"b":1}'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJson

        serializer = ToJson(
            json_encoder_kwargs={"sort_keys": True, "separators": (",", ":")}
        )
        text = serializer.to_json({"b": 1, "a": 2})
        # '{"a":2,"b":1}'
    """

    def __init__(
        self,
        *args: Any,
        depth: int = -1,
        json_encoder_cls: type[JSONEncoder] | None = None,
        json_encoder_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self.depth = depth
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

    def to_json(self, data: JSONRawValue) -> str:
        """
        Encode a value as JSON text with the configured encoder and options.
        """

        return dumps(data, cls=self.json_encoder_cls, **self.json_encoder_kwargs)


class ToJson(
    BaseToJsonProducer,
    BaseFlatMap[
        JSONValue,
        str,
    ],
):
    """
    Encode each input value as a stream of JSON text fragments.

    This is the usual entry point for JSON encoding. It selects the appropriate
    serializers for nested values automatically; most callers only need this
    stage and a sink for its output.

    Mappings become objects; other collections, including tuples and sets, become
    arrays. Strings are encoded as JSON strings. Nested values are serialized
    recursively, and :class:`BaseToJsonProducer` values contribute their existing
    fragments without being encoded again. Forwarded producers retain their own
    encoder settings. Sets follow their iteration order.

    Join the fragments for a complete document. Successive input values are
    concatenated without separators; use :class:`ToJsonPerLine` for JSON Lines,
    or :class:`ToJsonList` to wrap the input stream in one array. Unsupported
    values raise :class:`TypeError` and put the stage in the error state.

    At depth zero, mappings are passed directly to :func:`json.dumps`, preserving
    their keys and values as objects. Strings and scalar values are also encoded
    directly; other collections are converted to lists first. Encoder options
    such as ``sort_keys`` and ``indent`` apply to the complete directly encoded
    value. Recursive streaming preserves mapping iteration order instead of
    sorting its entries.

    Embedded producers inside directly encoded mappings or collections require
    a custom encoder. A producer supplied as the input value is forwarded at any
    depth. Empty input produces no fragments.

    :param depth: Recursive streaming budget; defaults to ``-1`` (unlimited).
        See :class:`BaseToJsonProducer` for how positive budgets are reduced.
    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`, for
        example ``{"sort_keys": True}``. Defaults to none.

    Input and output::

        input:  {"a": [1, 2]}, 3
        output joined: '{"a": [1, 2]}3'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJson
        from pyrill.sources import IterableSource

        source = IterableSource(source=[{"a": [1, 2]}, 3])
        stage = source >> ToJson()

        await stage.mount()
        text = "".join([fragment async for fragment in stage])
        # '{"a": [1, 2]}3'
    """

    def _map_frame_to_producer_no_deep(
        self,
        frame: JSONValue,
    ) -> BaseProducer[str]:
        match frame:
            case BaseToJsonProducer():
                return frame
            case str():
                return IterableSource(source=self.to_json(frame))
            case Mapping():
                return IterableSource(
                    source=self.to_json(cast(Mapping[str, JSONRawValue], frame))
                )
            case Collection():
                return IterableSource(
                    source=self.to_json(list(cast(Collection[JSONRawValue], frame)))
                )
            case _:
                return IterableSource(source=self.to_json(frame))

    async def _map_frame_to_producer(
        self,
        frame: JSONValue,
    ) -> BaseProducer[str]:
        if self.depth == 0:
            return self._map_frame_to_producer_no_deep(frame)

        match frame:
            case BaseToJsonProducer():
                return frame
            case Mapping():
                return IterableSource(
                    source=cast(Mapping[str, JSONValue], frame).items()
                ) >> ToJsonObject(
                    depth=max(0, self.depth - 1) if self.depth >= 0 else -1,
                    json_encoder_cls=self.json_encoder_cls,
                    json_encoder_kwargs=self.json_encoder_kwargs,
                )
            case str():
                return IterableSource(source=self.to_json(frame))
            case Collection():
                return IterableSource(
                    source=cast(Collection[JSONValue], frame)
                ) >> ToJsonList(
                    depth=max(0, self.depth - 1) if self.depth >= 0 else -1,
                    json_encoder_cls=self.json_encoder_cls,
                    json_encoder_kwargs=self.json_encoder_kwargs,
                )
            case _:
                return IterableSource(source=self.to_json(frame))


class ToJsonPerLine(ToJson):
    """
    Encode each input value as a JSON document followed by a newline.

    It works like :class:`ToJson`, but it forces compact separators (``","`` and
    ``":"``) and no indentation for values it serializes. It appends one newline
    after all fragments for each input value, even when the value is a JSON
    producer. Forwarded producers retain their own formatting. An empty input
    stream produces no text.

    :param depth: Recursive streaming budget, as in :class:`ToJson`.
        Defaults to ``-1`` (unlimited).
    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Extra keyword arguments for :func:`json.dumps`. The
        ``separators`` and ``indent`` options are always overridden.

    Input and output::

        input:  {"a": [1, 2]}, 3
        output joined: '{"a":[1,2]}\\n3\\n'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJsonPerLine
        from pyrill.sources import IterableSource

        source = IterableSource(source=[{"a": [1, 2]}, 3])
        stage = source >> ToJsonPerLine()

        await stage.mount()
        text = "".join([fragment async for fragment in stage])
        # '{"a":[1,2]}\\n3\\n'
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        self.json_encoder_kwargs["separators"] = (",", ":")
        self.json_encoder_kwargs["indent"] = None

    async def _map_frame_to_producer(
        self,
        frame: JSONValue,
    ) -> BaseProducer[str]:
        return (await super()._map_frame_to_producer(frame)) >> SuffixStream(
            suffix="\n"
        )


class BaseToJsonInner[Sink_co](BaseToJsonProducer, BaseFlatMap[Sink_co, str], ABC):
    """
    Serialize container entries with separators and without outer delimiters.

    Subclasses implement :meth:`_build_pipe` to return the producers that emit
    one entry. This base flattens those producers and inserts the configured
    item separator before every entry after the first. Mounting resets the
    first-entry flag. Empty input produces no fragments.

    Encoder settings and the depth budget are inherited from
    :class:`BaseToJsonProducer`; subclasses pass them to nested serializers.
    """

    _first: bool = False

    @abstractmethod
    def _build_pipe(self, frame: Sink_co) -> list[BaseProducer[str]]:
        raise NotImplementedError()

    async def _map_frame_to_producer(self, frame: Sink_co) -> BaseProducer[str]:
        pipe: list[BaseProducer[str]] = self._build_pipe(frame)
        if self._first is False:
            self._first = True
        else:
            pipe.insert(0, IterableSource(source=[self._separators()[0]]))

        return IterableSource(source=pipe) >> Flat[str]()

    async def _mount(self) -> None:
        self._first = False
        await super()._mount()


class BaseToJson[Source_co](BaseToJsonProducer, BaseStage[Source_co, str], ABC):
    """
    Wrap a stream of serialized entries in JSON container delimiters.

    Subclasses define ``_delimiters`` as an opening and closing string and
    ``_inner_stage`` as a :class:`BaseToJsonInner` implementation. The stage
    emits the opening delimiter, flattens the inner stage, then emits the closing
    delimiter. Empty input still produces both delimiters.

    The inner stage receives the source, depth budget and encoder settings.
    See :class:`BaseToJsonProducer` for the inherited constructor options.
    """

    _delimiters: tuple[str, str]
    _inner_stage: type[BaseToJsonInner[Source_co]]

    _source_iter: Iterator[BaseProducer[str]] | None = None

    _current_frame: BaseProducer[str] | None = None

    async def _next_frame(self) -> str:
        if self._source_iter is None:
            self._source_iter = iter(
                [
                    IterableSource(source=[self._delimiters[0]]),
                    self._inner_stage(
                        source=self.source,
                        depth=self.depth,
                        json_encoder_cls=self.json_encoder_cls,
                        json_encoder_kwargs=self.json_encoder_kwargs,
                    ),
                    IterableSource(source=[self._delimiters[1]]),
                ]
            )
        if self._current_frame is None:
            try:
                self._current_frame = next(self._source_iter)
            except StopIteration:
                raise StopAsyncIteration()

            await self._current_frame.mount()

        try:
            return await self._current_frame.__anext__()
        except StopAsyncIteration as ex:
            await self._current_frame.unmount()
            self._current_frame = None
            raise FrameSkipped(ex)

    async def _process_frame(self, frame: Source_co) -> str:
        raise NotImplementedError()


class ToJsonInnerList(BaseToJsonInner[JSONValue]):
    """
    Encode array items with separators and without square brackets.

    Each value is delegated to :class:`ToJson` with the inherited encoder
    settings and a reduced depth budget. Existing :class:`BaseToJsonProducer`
    values are forwarded with their own settings. Empty input produces no text.
    Use :class:`ToJsonList` to include the enclosing brackets.

    See :class:`BaseToJsonProducer` for the constructor options.

    Input and output::

        input:  1, "a", None
        output joined: '1, "a", null'
    """

    def _build_pipe(self, frame: JSONValue) -> list[BaseProducer[str]]:
        return [
            frame
            if isinstance(frame, BaseToJsonProducer)
            else IterableSource[JSONValue](source=[frame])
            >> ToJson(
                depth=max(0, self.depth - 1) if self.depth >= 0 else -1,
                json_encoder_cls=self.json_encoder_cls,
                json_encoder_kwargs=self.json_encoder_kwargs,
            )
        ]


class ToJsonList(BaseToJson[JSONValue]):
    """
    Encode the entire input stream as one JSON array, fragment by fragment.

    Concatenating the emitted frames gives the JSON text; nested lists, tuples,
    sets and mappings are serialized through :class:`ToJsonInnerList` and
    :class:`ToJson`. An empty input stream yields ``"[]"``. JSON producer values
    contribute their fragments directly and retain their own encoder settings.

    :param depth: Recursive streaming budget passed to the inner stage.
        Defaults to ``-1`` (unlimited); see :class:`BaseToJsonProducer`.
    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Options for :func:`json.dumps`, including the
        separators used between array items. Defaults to none.

    Input and output::

        input:  {"a": 1}, [2, 3]
        output joined: '[{"a": 1}, [2, 3]]'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJsonList
        from pyrill.sources import IterableSource

        stage = IterableSource(source=[1, [2, 3]]) >> ToJsonList()
        await stage.mount()
        text = "".join([fragment async for fragment in stage])
        # '[1, [2, 3]]'
    """

    _delimiters = ("[", "]")
    _inner_stage = ToJsonInnerList


class ToJsonInnerObject(BaseToJsonInner[tuple[str, JSONValue]]):
    """
    Encode key-value pairs with separators and without curly braces.

    Keys are JSON-escaped strings followed by the configured key separator.
    Values are delegated to :class:`ToJson` with the inherited encoder settings
    and a reduced depth budget. Existing :class:`BaseToJsonProducer` values are
    forwarded with their own settings. Empty input produces no text.
    Use :class:`ToJsonObject` to include the enclosing braces.

    See :class:`BaseToJsonProducer` for the constructor options.

    Input and output::

        input:  ("a", 1), ("b", None)
        output joined: '"a": 1, "b": null'
    """

    def _build_pipe(self, frame: tuple[str, JSONValue]) -> list[BaseProducer[str]]:
        return [
            IterableSource(source=[self.to_json(frame[0]), self._separators()[1]]),
            frame[1]
            if isinstance(frame[1], BaseToJsonProducer)
            else IterableSource(source=[frame[1]])
            >> ToJson(
                depth=max(0, self.depth - 1) if self.depth >= 0 else -1,
                json_encoder_cls=self.json_encoder_cls,
                json_encoder_kwargs=self.json_encoder_kwargs,
            ),
        ]


class ToJsonObject(BaseToJson[tuple[str, JSONValue]]):
    """
    Encode a stream of string-keyed pairs as one JSON object in fragments.

    Join the emitted fragments to obtain the complete object. Entries retain
    input order, including duplicate keys; ``sort_keys`` does not reorder the
    streamed entries. Keys are escaped by :class:`ToJsonInnerObject`, and nested
    values are serialized through :class:`ToJson`. JSON producer values forward
    their fragments with their own encoder settings. Empty input yields ``"{}"``.

    :param depth: Recursive streaming budget passed to the inner stage.
        Defaults to ``-1`` (unlimited); see :class:`BaseToJsonProducer`.
    :param json_encoder_cls: Custom :class:`json.JSONEncoder` subclass. Optional.
    :param json_encoder_kwargs: Options for :func:`json.dumps`, including the
        separators used between entries and between keys and values.
        Defaults to none.

    Input and output::

        input:  ("a", 1), ("b", [2, 3])
        output joined: '{"a": 1, "b": [2, 3]}'

    Usage:

    .. code-block:: python

        from pyrill.json import ToJsonObject
        from pyrill.sources import IterableSource

        stage = IterableSource(source=[("a", 1), ("b", [2, 3])]) >> ToJsonObject()
        await stage.mount()
        text = "".join([fragment async for fragment in stage])
        # '{"a": 1, "b": [2, 3]}'
    """

    _delimiters = ("{", "}")
    _inner_stage = ToJsonInnerObject

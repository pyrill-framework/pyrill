import json
from collections.abc import Collection
from typing import Any, cast

import pytest

from pyrill.base import BaseProducer, BaseStage, ElementState, Pipeline
from pyrill.json import (
    BaseToJsonProducer,
    FromJson,
    FromJsonMixin,
    JSONValue,
    ToJson,
    ToJsonInnerList,
    ToJsonInnerObject,
    ToJsonList,
    ToJsonObject,
    ToJsonPerLine,
)
from pyrill.sources import IterableSource

_ROUND_TRIP_ITEMS: list[list[Any]] = [
    [],
    [None, True, 1.5, 'quote"d'],
    [1, [2, [3, {"a": [4, {}]}]], [], {}],
]


async def _text(producer: BaseProducer[str]) -> str:
    await producer.mount()
    return "".join([fragment async for fragment in producer])


class TestFromJsonMixin:
    def test_decodes_nested_values(self) -> None:
        assert FromJsonMixin().from_json('{"k": [true, null, 2]}') == {
            "k": [True, None, 2]
        }

    def test_decoder_kwargs_are_used(self) -> None:
        decoder = FromJsonMixin(json_decoder_kwargs={"parse_float": str})

        assert decoder.from_json("[1.25]") == ["1.25"]

    def test_custom_decoder_is_used(self) -> None:
        class UpperDecoder(json.JSONDecoder):
            def decode(self, s: str, **kwargs: Any) -> Any:
                return super().decode(s, **kwargs).upper()

        decoder = FromJsonMixin(json_decoder_cls=UpperDecoder)

        assert decoder.from_json('"hello"') == "HELLO"

    def test_invalid_json_raises_decode_error(self) -> None:
        with pytest.raises(json.JSONDecodeError):
            FromJsonMixin().from_json("{")

    async def test_can_be_combined_with_a_custom_stage(self) -> None:
        class ExtractId(FromJsonMixin, BaseStage[str, int]):
            async def _process_frame(self, frame: str) -> int:
                return cast(dict[str, int], self.from_json(frame))["id"]

        stage = IterableSource(source=['{"id": 7}', '{"id": 8}']) >> ExtractId()
        await stage.mount()

        assert [value async for value in stage] == [7, 8]


class TestFromJson:
    @pytest.mark.parametrize(
        "value", [None, True, False, 12, 1.5, "café", [], {}, {"k": [1, None]}]
    )
    async def test_decodes_complete_documents(self, value: JSONValue) -> None:
        stage = IterableSource(source=[json.dumps(value)]) >> FromJson()
        await stage.mount()

        assert [frame async for frame in stage] == [value]
        assert stage.state is ElementState.NULL

    async def test_multiple_documents_preserve_frame_boundaries(self) -> None:
        stage = IterableSource(source=["1", "[2, 3]", "null"]) >> FromJson()
        await stage.mount()

        assert [frame async for frame in stage] == [1, [2, 3], None]

    async def test_empty_source_emits_no_values(self) -> None:
        stage = IterableSource[str](source=[]) >> FromJson()
        await stage.mount()

        assert [frame async for frame in stage] == []

    async def test_decoder_options_are_forwarded(self) -> None:
        stage = IterableSource(source=["1.25"]) >> FromJson(
            json_decoder_kwargs={"parse_float": str}
        )
        await stage.mount()

        assert [frame async for frame in stage] == ["1.25"]

    @pytest.mark.parametrize("document", ["", "{", "1 2", '["unfinished"'])
    async def test_invalid_document_sets_error_state(self, document: str) -> None:
        stage = IterableSource(source=[document]) >> FromJson()
        await stage.mount()

        with pytest.raises(json.JSONDecodeError):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR


class TestBaseToJsonProducer:
    def test_default_options_and_separators(self) -> None:
        producer = ToJson()

        assert producer.depth == -1
        assert producer.get_frame_separator() == ", "
        assert producer.get_key_separator() == ": "
        assert producer.to_json({"k": [1, 2]}) == '{"k": [1, 2]}'

    def test_custom_options_and_separators(self) -> None:
        producer = ToJson(
            depth=2,
            json_encoder_kwargs={
                "separators": (",", ":"),
                "ensure_ascii": False,
                "sort_keys": True,
            },
        )

        assert producer.depth == 2
        assert producer.get_frame_separator() == ","
        assert producer.get_key_separator() == ":"
        assert producer.to_json({"b": 1, "a": "é"}) == '{"a":"é","b":1}'

    def test_options_are_independent_between_instances(self) -> None:
        first = ToJson()
        second = ToJson()
        first.json_encoder_kwargs["ensure_ascii"] = False

        assert second.to_json("é") == '"\\u00e9"'


class TestToJsonPerLine:
    async def test_each_document_ends_with_one_newline(self) -> None:
        stage = (
            IterableSource[JSONValue](source=[{"k": [1, 2]}, "a\nb", None, []])
            >> ToJsonPerLine()
        )

        assert await _text(stage) == '{"k":[1,2]}\n"a\\nb"\nnull\n[]\n'

    async def test_empty_source_emits_no_newlines(self) -> None:
        stage = IterableSource[JSONValue](source=[]) >> ToJsonPerLine()

        assert await _text(stage) == ""

    async def test_forces_compact_output_and_preserves_other_options(self) -> None:
        stage = IterableSource[JSONValue](source=[{"k": "é"}]) >> ToJsonPerLine(
            json_encoder_kwargs={
                "indent": 4,
                "separators": (", ", ": "),
                "ensure_ascii": False,
            }
        )

        assert await _text(stage) == '{"k":"é"}\n'

    async def test_streamed_document_gets_one_trailing_newline(self) -> None:
        inner = IterableSource[JSONValue](source=[1, 2]) >> ToJsonList()
        stage = IterableSource[JSONValue](source=[inner]) >> ToJsonPerLine()

        assert await _text(stage) == "[1, 2]\n"


class TestToJsonInnerList:
    async def test_emits_items_and_separators_without_delimiters(self) -> None:
        stage = IterableSource[JSONValue](source=[1, "a", None]) >> ToJsonInnerList()

        assert await _text(stage) == '1, "a", null'

    async def test_empty_source_emits_no_text(self) -> None:
        stage = IterableSource[JSONValue](source=[]) >> ToJsonInnerList()

        assert await _text(stage) == ""

    async def test_embedded_producer_is_forwarded(self) -> None:
        inner = IterableSource[JSONValue](source=[2, 3]) >> ToJsonList()
        stage = IterableSource[JSONValue](source=[1, inner]) >> ToJsonInnerList(
            json_encoder_kwargs={"separators": (",", ":")}
        )

        assert await _text(stage) == "1,[2, 3]"

    async def test_remount_resets_first_item_separator(self) -> None:
        stage = IterableSource[JSONValue](source=[1, 2]) >> ToJsonInnerList()

        assert await _text(stage) == "1, 2"
        assert await _text(stage) == "1, 2"


class TestToJsonInnerObject:
    async def test_emits_escaped_keys_and_values_without_delimiters(self) -> None:
        stage = IterableSource[tuple[str, JSONValue]](
            source=[('k"ey', [1, 2]), ("empty", None)]
        ) >> ToJsonInnerObject(json_encoder_kwargs={"separators": (",", ":")})

        assert await _text(stage) == '"k\\"ey":[1,2],"empty":null'

    async def test_empty_source_emits_no_text(self) -> None:
        stage = IterableSource[tuple[str, JSONValue]](source=[]) >> ToJsonInnerObject()

        assert await _text(stage) == ""

    async def test_embedded_producer_is_forwarded(self) -> None:
        inner = IterableSource[JSONValue](source=[1, 2]) >> ToJsonList()
        stage = (
            IterableSource[tuple[str, JSONValue]](source=[("k", inner)])
            >> ToJsonInnerObject()
        )

        assert await _text(stage) == '"k": [1, 2]'

    async def test_remount_resets_first_item_separator(self) -> None:
        stage = (
            IterableSource[tuple[str, JSONValue]](source=[("a", 1), ("b", 2)])
            >> ToJsonInnerObject()
        )

        assert await _text(stage) == '"a": 1, "b": 2'
        assert await _text(stage) == '"a": 1, "b": 2'


class TestToJson:
    @pytest.mark.parametrize("depth", [-1, 0, 1])
    @pytest.mark.parametrize("value", ["", 'quote"d', "café\n"])
    async def test_strings_are_encoded_as_json_strings(
        self, value: str, depth: int
    ) -> None:
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(depth=depth)

        assert await _text(stage) == json.dumps(value)

    @pytest.mark.parametrize("depth", [-1, 0])
    @pytest.mark.parametrize("value", [None, True, False, 0, -123, 1.5])
    async def test_scalar_values_are_encoded(
        self, value: JSONValue, depth: int
    ) -> None:
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(depth=depth)

        assert await _text(stage) == json.dumps(value)
        assert stage.state is ElementState.NULL

    async def test_empty_source_emits_no_fragments(self) -> None:
        stage = IterableSource[JSONValue](source=[]) >> ToJson()

        assert await _text(stage) == ""
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ([], "[]"),
            ({}, "{}"),
            ([1, [2], {"k": None}], '[1, [2], {"k": null}]'),
            ({"a": [1, 2], "b": False}, '{"a": [1, 2], "b": false}'),
            (("key", 42), '["key", 42]'),
            ({3}, "[3]"),
        ],
        ids=["empty-list", "empty-object", "list", "mapping", "tuple", "set"],
    )
    async def test_containers_are_streamed(self, value: Any, expected: str) -> None:
        stage = IterableSource(source=[value]) >> ToJson()

        assert await _text(stage) == expected

    async def test_multiple_input_frames_are_encoded_in_order(self) -> None:
        stage = IterableSource[JSONValue](source=[12, None, [3]]) >> ToJson()

        assert await _text(stage) == "12null[3]"

    @pytest.mark.parametrize("depth", [-1, 0, 1])
    async def test_json_producer_fragments_are_forwarded(self, depth: int) -> None:
        inner = IterableSource[JSONValue](source=[1, 2]) >> ToJsonList()
        stage = IterableSource(source=[inner]) >> ToJson(depth=depth)
        await stage.mount()

        assert [fragment async for fragment in stage] == ["[", "1", ", ", "2", "]"]

    @pytest.mark.parametrize("depth", [-1, 0, 1, 2])
    @pytest.mark.parametrize("value", [[1, [2, None]], (1, 2), {3}])
    async def test_collections_are_encoded_at_each_depth(
        self, value: Collection[JSONValue], depth: int
    ) -> None:
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(depth=depth)

        assert json.loads(await _text(stage)) == list(value)

    @pytest.mark.parametrize("depth", [-1, 0, 1, 2])
    @pytest.mark.parametrize(
        "value",
        [{}, {"a": 1, "b": None}, {"outer": {"inner": [1, {"k": True}]}}],
        ids=["empty", "scalars", "nested"],
    )
    async def test_mappings_preserve_keys_and_values_at_each_depth(
        self, value: dict[str, JSONValue], depth: int
    ) -> None:
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(depth=depth)

        assert json.loads(await _text(stage)) == value

    async def test_depth_zero_applies_encoder_options_to_complete_mapping(self) -> None:
        value: dict[str, JSONValue] = {"b": {"d": "é", "c": 1}, "a": [2, 3]}
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(
            depth=0,
            json_encoder_kwargs={"sort_keys": True, "indent": 2, "ensure_ascii": False},
        )

        assert await _text(stage) == json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=False,
            separators=(", ", ": "),
        )

    @pytest.mark.parametrize("container", ["list", "mapping"])
    async def test_depth_zero_rejects_embedded_producers_without_custom_encoder(
        self, container: str
    ) -> None:
        inner = IterableSource[JSONValue](source=[1, 2]) >> ToJsonList()
        value: JSONValue = [inner] if container == "list" else {"k": inner}
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(depth=0)
        await stage.mount()

        with pytest.raises(TypeError, match="not JSON serializable"):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR

    @pytest.mark.parametrize("depth", [-1, 1, 2])
    @pytest.mark.parametrize("container", ["list", "mapping"])
    async def test_nested_json_producers_are_flattened(
        self, depth: int, container: str
    ) -> None:
        inner = IterableSource[JSONValue](source=[1, 2]) >> ToJsonList()
        value: JSONValue = [inner] if container == "list" else {"k": inner}
        stage = IterableSource[JSONValue](source=[value]) >> ToJson(depth=depth)

        expected = [[1, 2]] if container == "list" else {"k": [1, 2]}
        assert json.loads(await _text(stage)) == expected

    async def test_encoder_kwargs_apply_to_scalar_values(self) -> None:
        stage = IterableSource[JSONValue](source=[float("nan")]) >> ToJson(
            json_encoder_kwargs={"allow_nan": False}
        )
        await stage.mount()

        with pytest.raises(ValueError, match="Out of range float values"):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR

    async def test_encoder_kwargs_apply_to_nested_containers(self) -> None:
        stage = IterableSource[JSONValue](source=[{"k": [1, 2]}]) >> ToJson(
            json_encoder_kwargs={"separators": (",", ":")}
        )

        assert await _text(stage) == '{"k":[1,2]}'

    @pytest.mark.parametrize("depth", [-1, 0, 1])
    async def test_custom_encoder_is_used(self, depth: int) -> None:
        class ComplexEncoder(json.JSONEncoder):
            def default(self, o: Any) -> Any:
                if isinstance(o, complex):
                    return [o.real, o.imag]
                return super().default(o)

        stage = IterableSource(source=[cast(JSONValue, 1 + 2j)]) >> ToJson(
            depth=depth, json_encoder_cls=ComplexEncoder
        )

        assert await _text(stage) == "[1.0, 2.0]"

    async def test_unserializable_value_sets_error_state(self) -> None:
        stage = IterableSource(source=[cast(JSONValue, object())]) >> ToJson()
        await stage.mount()

        with pytest.raises(TypeError, match="not JSON serializable"):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR


# ---------------------------------------------------------------------------
# TestToJsonList
# ---------------------------------------------------------------------------


class TestToJsonList:
    async def test_fragments_join_into_json_array(self) -> None:
        source: IterableSource[JSONValue] = IterableSource(
            source=[{"field_1": "id1", "field_2": 1}, ["id2", 2, "pong_2"]]
        )
        stage = source >> ToJsonList()

        result = await _text(stage)

        assert result == '[{"field_1": "id1", "field_2": 1}, ["id2", 2, "pong_2"]]'
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize(
        "items", _ROUND_TRIP_ITEMS, ids=["empty", "scalars", "nested"]
    )
    async def test_output_decodes_to_the_input_items(self, items: list[Any]) -> None:
        stage = IterableSource[JSONValue](source=items) >> ToJsonList()

        result = await _text(stage)

        assert json.loads(result) == items

    async def test_encoder_kwargs_apply_to_nested_values(self) -> None:
        stage = IterableSource[JSONValue](source=[[1, 2], {"k": 3}]) >> ToJsonList(
            json_encoder_kwargs={"separators": (",", ":")}
        )

        result = await _text(stage)

        assert result == '[[1,2],{"k":3}]'

    async def test_nested_values_use_the_stage_pipeline(self) -> None:
        pipeline = Pipeline("own")
        source: IterableSource[JSONValue] = IterableSource(
            source=[[1], {"k": 2}], pipeline=pipeline
        )
        stage = source >> ToJsonList(pipeline=pipeline)

        result = await _text(stage)

        assert result == '[[1], {"k": 2}]'

    async def test_stream_items_are_flattened(self) -> None:
        items: list[BaseToJsonProducer] = [
            IterableSource[JSONValue](source=["a", "b"]) >> ToJsonList(),
            IterableSource[tuple[str, JSONValue]](source=[("k", 1)]) >> ToJsonObject(),
            IterableSource[JSONValue](source=[{"single": True}]) >> ToJson(),
        ]
        stage = IterableSource(source=items) >> ToJsonList()

        result = await _text(stage)

        assert result == '[["a", "b"], {"k": 1}, {"single": true}]'

    async def test_stream_items_nested_in_values_are_flattened(self) -> None:
        inner = IterableSource[JSONValue](source=[2, 3]) >> ToJsonList()
        items: list[BaseToJsonProducer] = [
            IterableSource(
                source={
                    "k": IterableSource[JSONValue](source=[1, inner]) >> ToJsonList()
                }.items()
            )
            >> ToJsonObject()
        ]
        stage = IterableSource(source=items) >> ToJsonList()

        result = await _text(stage)

        assert json.loads(result) == [{"k": [1, [2, 3]]}]

    async def test_mount_after_end_flattens_stream_items_again(self) -> None:
        items: list[BaseToJsonProducer] = [
            IterableSource[int](source=[1]) >> ToJsonList()
        ]
        stage = IterableSource(source=items) >> ToJsonList()

        result = await _text(stage)

        assert result == "[[1]]"

    async def test_mount_after_end_encodes_again(self) -> None:
        stage = IterableSource[JSONValue](source=[1, [2]]) >> ToJsonList()

        result = await _text(stage)

        assert result == "[1, [2]]"


# ---------------------------------------------------------------------------
# TestToJsonObject
# ---------------------------------------------------------------------------


class TestToJsonObject:
    async def test_fragments_join_into_json_object(self) -> None:
        source: IterableSource[tuple[str, JSONValue]] = IterableSource(
            source=[("a", 1), ("b", [2, {"c": 3}])]
        )
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert result == '{"a": 1, "b": [2, {"c": 3}]}'
        assert stage.state is ElementState.NULL

    async def test_empty_stream_gives_empty_object(self) -> None:
        source: IterableSource[tuple[str, JSONValue]] = IterableSource(source=[])
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert result == "{}"

    async def test_stream_values_are_flattened(self) -> None:
        source: IterableSource[tuple[str, JSONValue]] = IterableSource(
            source=[("a", IterableSource(source=[1, 2]) >> ToJsonList())]
        )
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert result == '{"a": [1, 2]}'

    async def test_keys_are_escaped(self) -> None:
        source: IterableSource[tuple[str, JSONValue]] = IterableSource(
            source=[('k"ey', None)]
        )
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert json.loads(result) == {'k"ey': None}

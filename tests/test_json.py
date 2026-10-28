import json
from typing import Any

import pytest

from pyrill.base import BaseProducer, ElementState, Pipeline
from pyrill.json import JsonableValue, JSONValue, ToJson, ToJsonList, ToJsonObject
from pyrill.sources import IterableSource

_ROUND_TRIP_ITEMS: list[list[Any]] = [
    [],
    [None, True, 1.5, 'quote"d'],
    [1, [2, [3, {"a": [4, {}]}]], [], {}],
]


async def _text(producer: BaseProducer[str]) -> str:
    await producer.mount()
    return "".join([fragment async for fragment in producer])


# ---------------------------------------------------------------------------
# TestToJsonList
# ---------------------------------------------------------------------------


class TestToJsonList:
    async def test_fragments_join_into_json_array(self) -> None:
        source: IterableSource[JsonableValue] = IterableSource(
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
        stage = IterableSource[JsonableValue](source=items) >> ToJsonList()

        result = await _text(stage)

        assert json.loads(result) == items

    async def test_tuples_and_sets_are_encoded_as_arrays(self) -> None:
        stage = IterableSource[JsonableValue](source=[(1, 2), {3}]) >> ToJsonList()

        result = await _text(stage)

        assert result == "[[1, 2], [3]]"

    async def test_encoder_kwargs_apply_to_nested_values(self) -> None:
        stage = IterableSource[JsonableValue](source=[[1, 2], {"k": 3}]) >> ToJsonList(
            json_encoder_kwargs={"separators": (",", ":")}
        )

        result = await _text(stage)

        assert result == '[[1,2],{"k":3}]'

    async def test_nested_values_use_the_stage_pipeline(self) -> None:
        pipeline = Pipeline("own")
        source: IterableSource[JsonableValue] = IterableSource(
            source=[[1], {"k": 2}], pipeline=pipeline
        )
        stage = source >> ToJsonList(pipeline=pipeline)

        result = await _text(stage)

        assert result == '[[1], {"k": 2}]'

    async def test_stream_items_are_flattened(self) -> None:
        items: list[JsonableValue] = [
            IterableSource(source=["a", "b"]) >> ToJsonList(),
            IterableSource(source=[("k", 1)]) >> ToJsonObject(),
            IterableSource[JSONValue](source=[{"single": True}]) >> ToJson(),
        ]
        stage = IterableSource(source=items) >> ToJsonList()

        result = await _text(stage)

        assert result == '[["a", "b"], {"k": 1}, {"single": true}]'

    async def test_stream_items_nested_in_values_are_flattened(self) -> None:
        inner = IterableSource[JsonableValue](source=[2, 3]) >> ToJsonList()
        items: list[JsonableValue] = [{"k": [1, inner]}]
        stage = IterableSource(source=items) >> ToJsonList()

        result = await _text(stage)

        assert json.loads(result) == [{"k": [1, [2, 3]]}]

    async def test_mount_after_end_flattens_stream_items_again(self) -> None:
        items: list[JsonableValue] = [IterableSource(source=[1]) >> ToJsonList()]
        stage = IterableSource(source=items) >> ToJsonList()
        _ = await _text(stage)

        result = await _text(stage)

        assert result == "[[1]]"

    async def test_mount_after_end_encodes_again(self) -> None:
        stage = IterableSource[JsonableValue](source=[1, [2]]) >> ToJsonList()
        _ = await _text(stage)

        result = await _text(stage)

        assert result == "[1, [2]]"


# ---------------------------------------------------------------------------
# TestToJsonObject
# ---------------------------------------------------------------------------


class TestToJsonObject:
    async def test_fragments_join_into_json_object(self) -> None:
        source: IterableSource[tuple[str, JsonableValue]] = IterableSource(
            source=[("a", 1), ("b", [2, {"c": 3}])]
        )
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert result == '{"a": 1, "b": [2, {"c": 3}]}'
        assert stage.state is ElementState.NULL

    async def test_empty_stream_gives_empty_object(self) -> None:
        source: IterableSource[tuple[str, JsonableValue]] = IterableSource(source=[])
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert result == "{}"

    async def test_stream_values_are_flattened(self) -> None:
        source: IterableSource[tuple[str, JsonableValue]] = IterableSource(
            source=[("a", IterableSource(source=[1, 2]) >> ToJsonList())]
        )
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert result == '{"a": [1, 2]}'

    async def test_keys_are_escaped(self) -> None:
        source: IterableSource[tuple[str, JsonableValue]] = IterableSource(
            source=[('k"ey', None)]
        )
        stage = source >> ToJsonObject()

        result = await _text(stage)

        assert json.loads(result) == {'k"ey': None}

from typing import Any

import pytest

from pyrill.base import ElementState
from pyrill.csv import DictFromCsv, DictToCsv, ListFromCsv, ListToCsv
from pyrill.sinks import Last
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestDictToCsv
# ---------------------------------------------------------------------------


class TestDictToCsv:
    async def test_columns_property_returns_configured_columns(self) -> None:
        stage: DictToCsv = DictToCsv(columns=["name", "age"])

        result = stage.columns

        assert result == {"name": ("name", None), "age": ("age", None)}

    async def test_emits_header_row_before_data(self) -> None:
        source: IterableSource[dict[str, Any]] = IterableSource(
            source=[{"name": "Alice"}]
        )
        stage: DictToCsv = source >> DictToCsv(columns=["name"])
        await stage.mount()

        result = await stage.__anext__()

        assert result == "name\r\n"

    async def test_emits_data_row_after_header(self) -> None:
        source: IterableSource[dict[str, Any]] = IterableSource(
            source=[{"name": "Alice", "age": 30}]
        )
        stage: DictToCsv = source >> DictToCsv(columns=["name", "age"])
        await stage.mount()
        _ = await stage.__anext__()  # consume header

        result = await stage.__anext__()

        assert result == "Alice,30\r\n"

    async def test_header_false_emits_data_row_first(self) -> None:
        source: IterableSource[dict[str, Any]] = IterableSource(
            source=[{"name": "Alice"}]
        )
        stage: DictToCsv = source >> DictToCsv(columns=["name"], header=False)
        await stage.mount()

        result = await stage.__anext__()

        assert result == "Alice\r\n"

    async def test_missing_key_produces_empty_field(self) -> None:
        source: IterableSource[dict[str, Any]] = IterableSource(
            source=[{"name": "Alice"}]
        )
        stage: DictToCsv = source >> DictToCsv(columns=["name", "age"], header=False)
        await stage.mount()

        result = await stage.__anext__()

        assert result == "Alice,\r\n"

    async def test_mapper_transforms_value(self) -> None:
        source: IterableSource[dict[str, Any]] = IterableSource(
            source=[{"val": 3.14159}]
        )
        stage: DictToCsv = source >> DictToCsv(
            columns=[("val", "val", lambda x: round(x, 2))], header=False
        )
        await stage.mount()

        result = await stage.__anext__()

        assert result == "3.14\r\n"

    async def test_multiple_rows_emitted_in_order(self) -> None:
        source: IterableSource[dict[str, Any]] = IterableSource(
            source=[{"name": "Alice"}, {"name": "Bob"}]
        )
        sink: Last[str] = (
            source >> DictToCsv(columns=["name"], header=False) >> Last[str]()
        )

        result = await sink.get_frame()

        assert result == "Bob\r\n"


# ---------------------------------------------------------------------------
# TestListToCsv
# ---------------------------------------------------------------------------


class TestListToCsv:
    async def test_error_raised_by_mapper_propagates(self) -> None:
        def add_one(value: int) -> int:
            return value + 1

        source: IterableSource[list[Any]] = IterableSource(source=[["a"]])
        stage: ListToCsv = source >> ListToCsv(
            columns=[("x", 0, add_one)], header=False
        )
        await stage.mount()

        with pytest.raises(TypeError):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR

    async def test_emits_header_row_before_data(self) -> None:
        source: IterableSource[list[Any]] = IterableSource(source=[["Alice", 30]])
        stage: ListToCsv = source >> ListToCsv(columns=["name", "age"])
        await stage.mount()

        result = await stage.__anext__()

        assert result == "name,age\r\n"

    async def test_emits_data_row_after_header(self) -> None:
        source: IterableSource[list[Any]] = IterableSource(source=[["Alice", 30]])
        stage: ListToCsv = source >> ListToCsv(columns=["name", "age"])
        await stage.mount()
        _ = await stage.__anext__()  # consume header

        result = await stage.__anext__()

        assert result == "Alice,30\r\n"

    async def test_header_false_emits_data_row_first(self) -> None:
        source: IterableSource[list[Any]] = IterableSource(source=[["Alice", 30]])
        stage: ListToCsv = source >> ListToCsv(columns=["name", "age"], header=False)
        await stage.mount()

        result = await stage.__anext__()

        assert result == "Alice,30\r\n"

    async def test_dict_columns_selects_fields_by_index(self) -> None:
        source: IterableSource[list[Any]] = IterableSource(
            source=[["Alice", 30, "eng"]]
        )
        stage: ListToCsv = source >> ListToCsv(
            columns={"name": 0, "role": 2}, header=False
        )
        await stage.mount()

        result = await stage.__anext__()

        assert result == "Alice,eng\r\n"

    async def test_multiple_rows_emitted_in_order(self) -> None:
        source: IterableSource[list[Any]] = IterableSource(source=[["Alice"], ["Bob"]])
        sink: Last[str] = (
            source >> ListToCsv(columns=["name"], header=False) >> Last[str]()
        )

        result = await sink.get_frame()

        assert result == "Bob\r\n"


# ---------------------------------------------------------------------------
# TestDictFromCsv
# ---------------------------------------------------------------------------


class TestDictFromCsv:
    async def test_parses_csv_row_to_dict(self) -> None:
        source: IterableSource[str] = IterableSource(source=["Alice,30"])
        stage: DictFromCsv = source >> DictFromCsv(
            csv_kwargs={"fieldnames": ["name", "age"]}
        )
        await stage.mount()

        result = await stage.__anext__()

        assert isinstance(result, dict)
        assert result == {"name": "Alice", "age": "30"}

    async def test_maps_applies_type_conversion(self) -> None:
        source: IterableSource[str] = IterableSource(source=["Alice,30"])
        stage: DictFromCsv = source >> DictFromCsv(
            csv_kwargs={"fieldnames": ["name", "age"]}, maps={"age": int}
        )
        await stage.mount()

        result = await stage.__anext__()

        assert isinstance(result, dict)
        assert result == {"name": "Alice", "age": 30}

    async def test_multiple_rows_parsed_independently(self) -> None:
        source: IterableSource[str] = IterableSource(source=["Alice,30", "Bob,25"])
        stage: DictFromCsv = source >> DictFromCsv(
            csv_kwargs={"fieldnames": ["name", "age"]}
        )
        await stage.mount()
        first = await stage.__anext__()
        second = await stage.__anext__()

        assert isinstance(first, dict)
        assert isinstance(second, dict)
        assert first == {"name": "Alice", "age": "30"}
        assert second == {"name": "Bob", "age": "25"}

    async def test_quoted_fields_parsed_correctly(self) -> None:
        source: IterableSource[str] = IterableSource(source=['"Smith, Jr.",42'])
        stage: DictFromCsv = source >> DictFromCsv(
            csv_kwargs={"fieldnames": ["name", "age"]}
        )
        await stage.mount()

        result = await stage.__anext__()

        assert isinstance(result, dict)
        assert result["name"] == "Smith, Jr."


# ---------------------------------------------------------------------------
# TestListFromCsv
# ---------------------------------------------------------------------------


class TestListFromCsv:
    async def test_frame_with_several_rows_emits_each_row(self) -> None:
        source: IterableSource[str] = IterableSource(source=["1,2\n3,4\n", "5,6\n"])
        stage: ListFromCsv = source >> ListFromCsv()
        await stage.mount()

        result = [row async for row in stage]

        assert result == [["1", "2"], ["3", "4"], ["5", "6"]]
        assert stage.state is ElementState.NULL

    async def test_parses_csv_row_to_list(self) -> None:
        source: IterableSource[str] = IterableSource(source=["Alice,30"])
        stage: ListFromCsv = source >> ListFromCsv()
        await stage.mount()

        result = await stage.__anext__()

        assert isinstance(result, list)
        assert result == ["Alice", "30"]

    async def test_maps_applies_type_conversion(self) -> None:
        source: IterableSource[str] = IterableSource(source=["Alice,30"])
        stage: ListFromCsv = source >> ListFromCsv(maps={1: int})
        await stage.mount()

        result = await stage.__anext__()

        assert isinstance(result, list)
        assert result == ["Alice", 30]

    async def test_multiple_rows_parsed_independently(self) -> None:
        source: IterableSource[str] = IterableSource(source=["Alice,30", "Bob,25"])
        stage: ListFromCsv = source >> ListFromCsv()
        await stage.mount()
        first = await stage.__anext__()
        second = await stage.__anext__()

        assert isinstance(first, list)
        assert isinstance(second, list)
        assert first == ["Alice", "30"]
        assert second == ["Bob", "25"]

    async def test_quoted_fields_parsed_correctly(self) -> None:
        source: IterableSource[str] = IterableSource(source=['"Smith, Jr.",42'])
        stage: ListFromCsv = source >> ListFromCsv()
        await stage.mount()

        result = await stage.__anext__()

        assert isinstance(result, list)
        assert result[0] == "Smith, Jr."

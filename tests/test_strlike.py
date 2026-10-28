from typing import cast

import pytest

from pyrill.mappers import BaseMap
from pyrill.sinks import Last
from pyrill.sources import IterableSource
from pyrill.strlike import Join, LStrip, Replace, RStrip, Split, Strip

# ---------------------------------------------------------------------------
# TestSplit
# ---------------------------------------------------------------------------


class TestSplit:
    async def test_splits_by_whitespace_when_no_sep_given(self) -> None:
        source: IterableSource[str] = IterableSource(source=["hello world"])
        sink: Last[list[str]] = source >> Split() >> Last[list[str]]()

        result: list[str] = await sink.get_frame()

        assert result == ["hello", "world"]

    async def test_splits_by_explicit_separator(self) -> None:
        source: IterableSource[str] = IterableSource(source=["a,b,c"])
        sink: Last[list[str]] = source >> Split(sep=",") >> Last[list[str]]()

        result: list[str] = await sink.get_frame()

        assert result == ["a", "b", "c"]

    async def test_maxsplit_limits_number_of_splits(self) -> None:
        source: IterableSource[str] = IterableSource(source=["a,b,c,d"])
        sink: Last[list[str]] = (
            source >> Split(sep=",", maxsplit=2) >> Last[list[str]]()
        )

        result: list[str] = await sink.get_frame()

        assert result == ["a", "b", "c,d"]

    async def test_applied_to_each_frame_independently(self) -> None:
        source: IterableSource[str] = IterableSource(source=["a b", "c d"])
        stage: BaseMap[str, list[str]] = source >> Split[str]()
        await stage.mount()

        first: list[str] = await stage.__anext__()
        second: list[str] = await stage.__anext__()

        assert first == ["a", "b"]
        assert second == ["c", "d"]

    async def test_works_with_bytes(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"a:b:c"])
        sink: Last[list[bytes]] = source >> Split(sep=b":") >> Last[list[bytes]]()

        result: list[bytes] = await sink.get_frame()

        assert result == [b"a", b"b", b"c"]

    async def test_single_token_returns_one_element_list(self) -> None:
        source: IterableSource[str] = IterableSource(source=["hello"])
        sink: Last[list[str]] = source >> Split() >> Last[list[str]]()

        result: list[str] = await sink.get_frame()

        assert result == ["hello"]


# ---------------------------------------------------------------------------
# TestJoin
# ---------------------------------------------------------------------------


class TestJoin:
    async def test_joins_list_with_separator(self) -> None:
        source: IterableSource[list[str]] = IterableSource(source=[["a", "b", "c"]])
        sink: Last[str] = source >> Join(join_str=",") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "a,b,c"

    async def test_joins_with_empty_separator(self) -> None:
        source: IterableSource[list[str]] = IterableSource(source=[["x", "y", "z"]])
        sink: Last[str] = source >> Join(join_str="") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "xyz"

    async def test_joins_each_frame_independently(self) -> None:
        source: IterableSource[list[str]] = IterableSource(
            source=[["a", "b"], ["c", "d"]]
        )
        stage: BaseMap[list[str], str] = cast(
            BaseMap[list[str], str], source >> Join(join_str="-")
        )
        await stage.mount()

        first: str = await stage.__anext__()
        second: str = await stage.__anext__()

        assert first == "a-b"
        assert second == "c-d"

    async def test_join_str_none_raises_runtime_error(self) -> None:
        source: IterableSource[list[str]] = IterableSource(source=[["a", "b"]])
        stage: BaseMap[list[str], str] = cast(BaseMap[list[str], str], source >> Join())
        await stage.mount()

        with pytest.raises(RuntimeError, match="Join string must be set"):
            await stage.__anext__()

    async def test_works_with_bytes(self) -> None:
        source: IterableSource[list[bytes]] = IterableSource(source=[[b"a", b"b"]])
        sink: Last[bytes] = source >> Join(join_str=b":") >> Last[bytes]()

        result: bytes = await sink.get_frame()

        assert result == b"a:b"


# ---------------------------------------------------------------------------
# TestReplace
# ---------------------------------------------------------------------------


class TestReplace:
    async def test_replaces_all_occurrences_by_default(self) -> None:
        source: IterableSource[str] = IterableSource(source=["banana"])
        sink: Last[str] = source >> Replace(old="a", new="o") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "bonono"

    async def test_count_limits_number_of_replacements(self) -> None:
        source: IterableSource[str] = IterableSource(source=["aaa"])
        sink: Last[str] = source >> Replace(old="a", new="b", count=2) >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "bba"

    async def test_new_none_deletes_occurrences(self) -> None:
        source: IterableSource[str] = IterableSource(source=["banana"])
        sink: Last[str] = source >> Replace(old="a") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "bnn"

    async def test_old_none_is_no_op(self) -> None:
        source: IterableSource[str] = IterableSource(source=["hello"])
        sink: Last[str] = source >> Replace() >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "hello"

    async def test_works_with_bytes(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"hello"])
        sink: Last[bytes] = source >> Replace(old=b"l", new=b"r") >> Last[bytes]()

        result: bytes = await sink.get_frame()

        assert result == b"herro"


# ---------------------------------------------------------------------------
# TestStrip
# ---------------------------------------------------------------------------


class TestStrip:
    async def test_strips_whitespace_by_default(self) -> None:
        source: IterableSource[str] = IterableSource(source=["  hello  "])
        sink: Last[str] = source >> Strip() >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "hello"

    async def test_strips_specified_chars_from_both_ends(self) -> None:
        source: IterableSource[str] = IterableSource(source=["--hello--"])
        sink: Last[str] = source >> Strip(chars="-") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "hello"

    async def test_works_with_bytes(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"  hi  "])
        sink: Last[bytes] = source >> Strip() >> Last[bytes]()

        result: bytes = await sink.get_frame()

        assert result == b"hi"


# ---------------------------------------------------------------------------
# TestRStrip
# ---------------------------------------------------------------------------


class TestRStrip:
    async def test_strips_trailing_whitespace_only(self) -> None:
        source: IterableSource[str] = IterableSource(source=["  hello  "])
        sink: Last[str] = source >> RStrip() >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "  hello"

    async def test_strips_specified_chars_from_right_only(self) -> None:
        source: IterableSource[str] = IterableSource(source=["--hello--"])
        sink: Last[str] = source >> RStrip(chars="-") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "--hello"

    async def test_works_with_bytes(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"hi  "])
        sink: Last[bytes] = source >> RStrip() >> Last[bytes]()

        result: bytes = await sink.get_frame()

        assert result == b"hi"


# ---------------------------------------------------------------------------
# TestLStrip
# ---------------------------------------------------------------------------


class TestLStrip:
    async def test_strips_leading_whitespace_only(self) -> None:
        source: IterableSource[str] = IterableSource(source=["  hello  "])
        sink: Last[str] = source >> LStrip() >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "hello  "

    async def test_strips_specified_chars_from_left_only(self) -> None:
        source: IterableSource[str] = IterableSource(source=["--hello--"])
        sink: Last[str] = source >> LStrip(chars="-") >> Last[str]()

        result: str = await sink.get_frame()

        assert result == "hello--"

    async def test_works_with_bytes(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"  hi"])
        sink: Last[bytes] = source >> LStrip() >> Last[bytes]()

        result: bytes = await sink.get_frame()

        assert result == b"hi"

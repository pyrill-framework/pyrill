import pytest

from pyrill.base import ElementState
from pyrill.sinks import BlackHole, First, Last
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestFirst
# ---------------------------------------------------------------------------


class TestFirst:
    async def test_get_frame_returns_first_element_and_unmounts_both(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: First[int] = source >> First[int]()

        result = await sink.get_frame()

        assert result == 1
        assert sink.state == ElementState.NULL
        assert source.state == ElementState.NULL

    async def test_get_frame_returns_first_of_single_element_source(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        sink: First[int] = source >> First[int]()

        result = await sink.get_frame()

        assert result == 42
        assert sink.state == ElementState.NULL

    async def test_get_frame_raises_value_error_on_empty_source_and_unmounts(
        self,
    ) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        sink: First[int] = source >> First[int]()

        with pytest.raises(ValueError, match="No frames consumed"):
            await sink.get_frame()

        assert sink.state == ElementState.NULL
        assert source.state == ElementState.NULL


# ---------------------------------------------------------------------------
# TestLast
# ---------------------------------------------------------------------------


class TestLast:
    async def test_get_frame_returns_last_element_and_unmounts_both(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: Last[int] = source >> Last[int]()

        result = await sink.get_frame()

        assert result == 3
        assert sink.state == ElementState.NULL
        assert source.state == ElementState.NULL

    async def test_get_frame_with_single_element_returns_it(self) -> None:
        source: IterableSource[int] = IterableSource(source=[99])
        sink: Last[int] = source >> Last[int]()

        result = await sink.get_frame()

        assert result == 99
        assert sink.state == ElementState.NULL

    async def test_get_frame_raises_value_error_on_empty_source_and_unmounts(
        self,
    ) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        sink: Last[int] = source >> Last[int]()

        with pytest.raises(ValueError, match="No frames consumed"):
            await sink.get_frame()

        assert sink.state == ElementState.NULL
        assert source.state == ElementState.NULL


# ---------------------------------------------------------------------------
# TestBlackHole
# ---------------------------------------------------------------------------


class TestBlackHole:
    async def test_consume_all_discards_all_frames_silently(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: BlackHole[int] = source >> BlackHole[int]()

        await sink.consume_all()

        assert source.state == ElementState.NULL
        assert sink.state == ElementState.NULL

    async def test_consume_all_on_empty_source_completes_without_error(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        sink: BlackHole[int] = source >> BlackHole[int]()

        await sink.consume_all()

        assert source.state == ElementState.NULL
        assert sink.state == ElementState.NULL

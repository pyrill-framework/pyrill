import pytest

from pyrill.lists import FlatIterable, GetItem, GroupFrames
from pyrill.sinks import Last
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestFlatIterable
# ---------------------------------------------------------------------------


class TestFlatIterable:
    async def test_single_iterable_yields_individual_elements(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[1, 2, 3]])
        stage: FlatIterable[int] = source >> FlatIterable[int]()
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()
        third = await stage.__anext__()

        assert first == 1
        assert second == 2
        assert third == 3

    async def test_multiple_iterables_flattened_in_order(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[1, 2], [3, 4]])
        sink: Last[int] = source >> FlatIterable[int]() >> Last[int]()

        result = await sink.get_frame()

        assert result == 4

    async def test_all_elements_from_all_iterables_emitted(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[10, 20], [30]])
        stage: FlatIterable[int] = source >> FlatIterable[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == [10, 20, 30]

    async def test_empty_inner_iterable_is_skipped(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[], [5, 6]])
        stage: FlatIterable[int] = source >> FlatIterable[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == [5, 6]

    async def test_all_empty_iterables_exhausts_source(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[], []])
        stage: FlatIterable[int] = source >> FlatIterable[int]()
        await stage.mount()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_string_iterable_yields_characters(self) -> None:
        source: IterableSource[str] = IterableSource(source=["ab", "c"])
        stage: FlatIterable[str] = source >> FlatIterable[str]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# TestGroupFrames
# ---------------------------------------------------------------------------


class TestGroupFrames:
    async def test_max_length_batches_frames_into_fixed_size_lists(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3, 4])
        stage: GroupFrames[int] = source >> GroupFrames[int](max_length=2)
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()

        assert first == [1, 2]
        assert second == [3, 4]

    async def test_partial_batch_emitted_when_source_exhausted(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage: GroupFrames[int] = source >> GroupFrames[int](max_length=2)
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()

        assert first == [1, 2]
        assert second == [3]

    async def test_zero_max_length_collects_all_frames_into_one_list(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3, 4, 5])
        sink: Last[list[int]] = (
            source >> GroupFrames[int](max_length=0) >> Last[list[int]]()
        )

        result = await sink.get_frame()

        assert result == [1, 2, 3, 4, 5]

    async def test_empty_source_raises_stop_async_iteration(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        stage: GroupFrames[int] = source >> GroupFrames[int](max_length=3)
        await stage.mount()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_single_frame_source_yields_one_element_list(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        sink: Last[list[int]] = (
            source >> GroupFrames[int](max_length=0) >> Last[list[int]]()
        )

        result = await sink.get_frame()

        assert result == [42]

    async def test_max_length_larger_than_source_returns_all_in_one_batch(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        sink: Last[list[int]] = (
            source >> GroupFrames[int](max_length=10) >> Last[list[int]]()
        )

        result = await sink.get_frame()

        assert result == [1, 2]


# ---------------------------------------------------------------------------
# TestGetItem
# ---------------------------------------------------------------------------


class TestGetItem:
    async def test_default_index_zero_returns_first_element(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[10, 20, 30]])
        stage: GetItem[int] = source >> GetItem[int]()
        await stage.mount()

        result = await stage.__anext__()

        assert result == 10

    async def test_explicit_index_returns_element_at_that_position(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[10, 20, 30]])
        stage: GetItem[int] = source >> GetItem[int](index=2)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 30

    async def test_negative_index_returns_element_from_end(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[1, 2, 3]])
        stage: GetItem[int] = source >> GetItem[int](index=-1)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 3

    async def test_index_applied_to_each_frame(self) -> None:
        source: IterableSource[list[int]] = IterableSource(
            source=[[1, 2], [3, 4], [5, 6]]
        )
        stage: GetItem[int] = source >> GetItem[int](index=1)
        await stage.mount()

        result = [item async for item in stage]

        assert result == [2, 4, 6]

    async def test_index_out_of_range_raises_index_error(self) -> None:
        source: IterableSource[list[int]] = IterableSource(source=[[1, 2]])
        stage: GetItem[int] = source >> GetItem[int](index=5)
        await stage.mount()

        with pytest.raises(IndexError):
            await stage.__anext__()

    async def test_works_with_string_sequences(self) -> None:
        source: IterableSource[str] = IterableSource(source=["hello", "world"])
        sink: Last[str] = source >> GetItem[str](index=1) >> Last[str]()

        result = await sink.get_frame()

        assert result == "o"

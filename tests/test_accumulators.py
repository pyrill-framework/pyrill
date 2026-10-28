from pyrill.accumulators import Count, ListAcc, Size, SumAcc
from pyrill.sinks import Last
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestSumAcc
# ---------------------------------------------------------------------------


class TestSumAcc:
    async def test_sums_integer_frames(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: Last[int] = source >> SumAcc[int]() >> Last[int]()

        result = await sink.get_frame()

        assert result == 6

    async def test_each_frame_emits_running_total(self) -> None:
        source: IterableSource[int] = IterableSource(source=[10, 20, 30])
        stage: SumAcc[int] = source >> SumAcc[int]()
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()
        third = await stage.__anext__()

        assert first == 10
        assert second == 30
        assert third == 60

    async def test_initial_value_seeds_accumulator(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        sink: Last[int] = source >> SumAcc[int](initial_value=100) >> Last[int]()

        result = await sink.get_frame()

        assert result == 103

    async def test_sums_float_frames(self) -> None:
        source: IterableSource[float] = IterableSource(source=[1.5, 2.5])
        sink: Last[float] = source >> SumAcc[float]() >> Last[float]()

        result = await sink.get_frame()

        assert result == 4.0

    async def test_sums_string_frames(self) -> None:
        source: IterableSource[str] = IterableSource(source=["a", "b", "c"])
        sink: Last[str] = source >> SumAcc[str]() >> Last[str]()

        result = await sink.get_frame()

        assert result == "abc"

    async def test_single_frame_returns_that_frame(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        sink: Last[int] = source >> SumAcc[int]() >> Last[int]()

        result = await sink.get_frame()

        assert result == 42

    async def test_remount_resets_accumulator(self) -> None:
        source: IterableSource[int] = IterableSource(source=[5])
        stage: SumAcc[int] = source >> SumAcc[int]()
        await stage.mount()
        await stage.__anext__()
        await stage.unmount()

        source2: IterableSource[int] = IterableSource(source=[3])
        _ = source2 >> stage
        await stage.mount()
        result = await stage.__anext__()

        assert result == 3


# ---------------------------------------------------------------------------
# TestListAcc
# ---------------------------------------------------------------------------


class TestListAcc:
    async def test_each_frame_emits_accumulated_list(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage: ListAcc[int] = source >> ListAcc[int]()
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()
        third = await stage.__anext__()

        assert first == [1]
        assert second == [1, 2]
        assert third == [1, 2, 3]

    async def test_final_frame_is_complete_list(self) -> None:
        source: IterableSource[str] = IterableSource(source=["a", "b", "c"])
        sink: Last[list[str]] = source >> ListAcc[str]() >> Last[list[str]]()

        result = await sink.get_frame()

        assert result == ["a", "b", "c"]

    async def test_emitted_list_is_a_copy(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        stage: ListAcc[int] = source >> ListAcc[int]()
        await stage.mount()

        snapshot1 = await stage.__anext__()
        snapshot2 = await stage.__anext__()

        assert snapshot1 == [1]
        assert snapshot1 is not snapshot2

    async def test_single_frame_yields_single_element_list(self) -> None:
        source: IterableSource[int] = IterableSource(source=[7])
        sink: Last[list[int]] = source >> ListAcc[int]() >> Last[list[int]]()

        result = await sink.get_frame()

        assert result == [7]

    async def test_remount_resets_list(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        stage: ListAcc[int] = source >> ListAcc[int]()
        await stage.mount()
        await stage.__anext__()
        await stage.unmount()

        source2: IterableSource[int] = IterableSource(source=[9])
        _ = source2 >> stage
        await stage.mount()
        result = await stage.__anext__()

        assert result == [9]


# ---------------------------------------------------------------------------
# TestCount
# ---------------------------------------------------------------------------


class TestCount:
    async def test_increments_counter_per_frame(self) -> None:
        source: IterableSource[int] = IterableSource(source=[10, 20, 30])
        stage: Count = source >> Count()
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()
        third = await stage.__anext__()

        assert first == 1
        assert second == 2
        assert third == 3

    async def test_final_count_equals_source_length(self) -> None:
        source: IterableSource[str] = IterableSource(source=["a", "b", "c", "d"])
        sink: Last[int] = source >> Count() >> Last[int]()

        result = await sink.get_frame()

        assert result == 4

    async def test_counts_frames_regardless_of_value(self) -> None:
        source: IterableSource[int] = IterableSource(source=[0, 0, 0])
        sink: Last[int] = source >> Count() >> Last[int]()

        result = await sink.get_frame()

        assert result == 3

    async def test_remount_resets_counter(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage: Count = source >> Count()
        await stage.mount()
        await stage.__anext__()
        await stage.__anext__()
        await stage.__anext__()
        await stage.unmount()

        source2: IterableSource[int] = IterableSource(source=[99])
        _ = source2 >> stage
        await stage.mount()
        result = await stage.__anext__()

        assert result == 1


# ---------------------------------------------------------------------------
# TestSize
# ---------------------------------------------------------------------------


class TestSize:
    async def test_accumulates_length_of_each_frame(self) -> None:
        source: IterableSource[str] = IterableSource(source=["ab", "cde"])
        stage: Size = source >> Size()
        await stage.mount()

        first = await stage.__anext__()
        second = await stage.__anext__()

        assert first == 2
        assert second == 5

    async def test_final_value_is_total_length(self) -> None:
        source: IterableSource[list[int]] = IterableSource(
            source=[[1, 2], [3], [4, 5, 6]]
        )
        sink: Last[int] = source >> Size() >> Last[int]()

        result = await sink.get_frame()

        assert result == 6

    async def test_single_frame_returns_its_length(self) -> None:
        source: IterableSource[str] = IterableSource(source=["hello"])
        sink: Last[int] = source >> Size() >> Last[int]()

        result = await sink.get_frame()

        assert result == 5

    async def test_remount_resets_accumulator(self) -> None:
        source: IterableSource[str] = IterableSource(source=["abc"])
        stage: Size = source >> Size()
        await stage.mount()
        await stage.__anext__()
        await stage.unmount()

        source2: IterableSource[str] = IterableSource(source=["xy"])
        _ = source2 >> stage
        await stage.mount()
        result = await stage.__anext__()

        assert result == 2

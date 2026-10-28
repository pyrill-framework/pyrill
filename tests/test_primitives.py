import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import suppress
from typing import Any

import pytest

from pyrill.base import BaseConsumer, BaseProducer, ElementState, Pipeline
from pyrill.mappers import Map
from pyrill.primitives import (
    BaseBin,
    Bin,
    Flat,
    Gate,
    Noop,
    PrefixStream,
    Retry,
    SkipIf,
    SuffixStream,
    Tee,
    Union,
    Zip,
    bin_stage,
    zip_sources,
)
from pyrill.queues import QueueSource
from pyrill.sinks import First, Last
from pyrill.sources import AsyncIterableSource, IterableSource
from pyrill.strings import Upper
from pyrill.strlike import Strip

# ---------------------------------------------------------------------------
# TestNoop
# ---------------------------------------------------------------------------


class TestNoop:
    async def test_single_frame_passes_through(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        stage = source >> Noop[int]()
        await stage.mount()

        result = await stage.__anext__()

        assert result == 42

    async def test_all_frames_pass_through_unchanged(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> Noop[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == [1, 2, 3]

    async def test_empty_source_yields_nothing(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        stage = source >> Noop[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == []
        assert stage.state == ElementState.NULL


# ---------------------------------------------------------------------------
# TestSkipIf
# ---------------------------------------------------------------------------


class TestSkipIf:
    async def test_passes_frame_when_sync_check_returns_true(self) -> None:
        source: IterableSource[int] = IterableSource(source=[5])
        stage = source >> SkipIf[int](check_func=lambda x: True)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 5

    async def test_skips_all_frames_when_sync_check_always_false(self) -> None:
        source: IterableSource[int] = IterableSource(source=[5, 10])
        stage = source >> SkipIf[int](check_func=lambda x: False)
        await stage.mount()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_passes_frame_when_async_check_returns_true(self) -> None:
        async def always_pass(x: int) -> bool:
            return True

        source: IterableSource[int] = IterableSource(source=[7])
        stage = source >> SkipIf[int](check_func=always_pass)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 7

    async def test_skips_all_frames_when_async_check_always_false(self) -> None:
        async def always_skip(x: int) -> bool:
            return False

        source: IterableSource[int] = IterableSource(source=[7, 8])
        stage = source >> SkipIf[int](check_func=always_skip)
        await stage.mount()

        with pytest.raises(StopAsyncIteration):
            await stage.__anext__()

    async def test_filters_frames_matching_predicate(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3, 4, 5])
        stage = source >> SkipIf[int](check_func=lambda x: x % 2 == 1)
        await stage.mount()

        result = [item async for item in stage]

        assert result == [1, 3, 5]


# ---------------------------------------------------------------------------
# TestTee
# ---------------------------------------------------------------------------
# Tee is a fan-out stage: each downstream consumer created via aiter(tee)
# (or by mounting a pipeline element whose source is tee) gets its own
# FutureSource that receives every pushed frame.  The background _consume()
# task reads from the input source and pushes each frame to all registered
# consumers concurrently.
#
# tee.mount() must be called before __aiter__() so that _consume() finds a
# live _iter; the simplest way is `await tee.mount()` in the test.
# ---------------------------------------------------------------------------


class TestTee:
    async def test_rshift_tuple_connects_each_consumer_and_returns_tuple(
        self,
    ) -> None:
        tee = IterableSource(source=[1]) >> Tee[int]()
        first = Last[int]()
        second = Last[int]()

        result = tee >> (first, second)

        assert result == (first, second)
        assert first.source is tee
        assert second.source is tee

    async def test_rshift_tuple_feeds_every_frame_to_each_branch(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        tens = Map[int, int](map_func=lambda frame: frame * 10)
        texts = Map[int, str](map_func=str)
        zipped = source >> Tee[int]() >> (tens, texts) >> Zip[int, str]()
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(10, "1"), (20, "2"), (30, "3")]

    async def test_full_chain_into_sink_yields_last_tuple(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink = (
            source
            >> Tee[int]()
            >> (
                Map[int, int](map_func=lambda frame: frame * 10),
                Map[int, str](map_func=str),
            )
            >> Zip[int, str]()
            >> Last[tuple[int, str]]()
        )

        result = await sink.get_frame()

        assert result == (30, "3")

    async def test_sinks_connected_with_both_operators_get_every_frame(self) -> None:
        tee = IterableSource(source=[1, 2, 3]) >> Tee[int]()
        right = tee >> Last[int]()
        left = Last[int]() << tee

        result = await asyncio.gather(right.get_frame(), left.get_frame())

        assert list(result) == [3, 3]

    async def test_branch_finishing_early_leaves_other_branches_running(
        self,
    ) -> None:
        tee = IterableSource(source=[1, 2, 3]) >> Tee[int]()
        first = tee >> First[int]()
        last = tee >> Last[int]()

        result = await asyncio.gather(first.get_frame(), last.get_frame())

        assert list(result) == [1, 3]

    async def test_rshift_tuple_from_different_pipeline_connects_nothing(
        self,
    ) -> None:
        tee = IterableSource(source=[1]) >> Tee[int]()
        local = Last[int]()
        foreign = Last[int](pipeline=Pipeline("other"))

        with pytest.raises(RuntimeError):
            _ = tee >> (local, foreign)

        assert local.source is None
        assert foreign.source is None

    async def test_rshift_tuple_with_non_consumer_raises_type_error(self) -> None:
        tee = IterableSource(source=[1]) >> Tee[int]()
        sink = Last[int]()

        with pytest.raises(TypeError):
            _ = tee >> (sink, 5)  # type: ignore[operator]

        assert sink.source is None

    async def test_consumers_reaching_end_leave_tee_unmounted(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        tee = source >> Tee[int]()
        await tee.mount()
        first = aiter(tee)
        second = aiter(tee)

        async def collect(it: AsyncIterator[int]) -> list[int]:
            return [frame async for frame in it]

        result = await asyncio.gather(collect(first), collect(second))
        await asyncio.sleep(0.05)  # Tee unmounts itself in an orphan task

        assert list(result) == [[1, 2], [1, 2]]
        assert tee.state is ElementState.NULL

    async def test_consumer_count_starts_at_zero(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        tee = source >> Tee[int]()

        assert tee.consumer_count() == 0

    async def test_consumer_count_increases_on_aiter(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        tee = source >> Tee[int]()
        await tee.mount()

        tee.__aiter__()
        tee.__aiter__()

        assert tee.consumer_count() == 2

    async def test_single_consumer_receives_all_frames(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        tee = source >> Tee[int]()
        await tee.mount()

        result = [item async for item in tee]

        assert result == [1, 2, 3]

    async def test_two_consumers_each_receive_all_frames(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        tee = source >> Tee[int]()
        await tee.mount()

        frames1: list[int] = []
        frames2: list[int] = []

        async def collect1() -> None:
            async for item in tee:
                frames1.append(item)

        async def collect2() -> None:
            async for item in tee:
                frames2.append(item)

        # Both consumers must run concurrently: _push_frame waits for all
        # consumers to receive each frame before proceeding.
        await asyncio.gather(
            asyncio.create_task(collect1()),
            asyncio.create_task(collect2()),
        )

        assert frames1 == [1, 2, 3]
        assert frames2 == [1, 2, 3]


# ---------------------------------------------------------------------------
# TestUnion
# ---------------------------------------------------------------------------


class TestUnion:
    async def test_anext_without_sources_ends_stream(self) -> None:
        union = Union[int]()
        await union.mount()

        with pytest.raises(StopAsyncIteration):
            await union.__anext__()

        assert union.state is ElementState.NULL

    async def test_source_count_starts_at_zero(self) -> None:
        union = Union[int]()

        assert union.source_count() == 0

    async def test_add_source_increments_count(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        union = Union[int]()

        union.add_source(source)

        assert union.source_count() == 1

    async def test_remove_source_decrements_count(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        union = Union[int]()
        union.add_source(source)

        union.remove_source(source)

        assert union.source_count() == 0

    async def test_add_source_from_different_pipeline_raises(self) -> None:
        p1 = Pipeline("p1")
        p2 = Pipeline("p2")
        source: IterableSource[int] = IterableSource(source=[1], pipeline=p1)
        union = Union[int](pipeline=p2)

        with pytest.raises(RuntimeError):
            union.add_source(source)

    async def test_yields_all_frames_from_single_source(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        union = Union[int]()
        union.add_source(source)
        await union.mount()

        result = [item async for item in union]

        assert sorted(result) == [1, 2, 3]

    async def test_yields_frames_from_multiple_sources(self) -> None:
        # Union stops as soon as any one source is exhausted; order is
        # non-deterministic because sources race concurrently.
        source1: IterableSource[int] = IterableSource(source=[1, 2])
        source2: IterableSource[int] = IterableSource(source=[3, 4])
        union = Union[int]()
        union.add_source(source1)
        union.add_source(source2)
        await union.mount()

        result = [item async for item in union]

        assert len(result) >= 1
        assert all(r in (1, 2, 3, 4) for r in result)


# ---------------------------------------------------------------------------
# TestZip
# ---------------------------------------------------------------------------


async def _failing_frames(frames: list[int]) -> AsyncIterator[int]:
    for frame in frames:
        yield frame
    raise ValueError("source failed")


def _zip_of[*Ts](zipped: Zip[*Ts], sources: Sequence[Sequence[object]]) -> Zip[*Ts]:
    for frames in sources:
        zipped.add_source(IterableSource(source=frames))
    return zipped


class TestZip:
    async def test_source_count_starts_at_zero(self) -> None:
        zipped = Zip[int]()

        assert zipped.source_count() == 0

    async def test_add_source_increments_count(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        zipped = Zip[int]()

        zipped.add_source(source)

        assert zipped.source_count() == 1

    async def test_remove_source_decrements_count(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        zipped = Zip[int]()
        zipped.add_source(source)

        zipped.remove_source(source)

        assert zipped.source_count() == 0

    async def test_add_source_from_different_pipeline_raises(self) -> None:
        source: IterableSource[int] = IterableSource(
            source=[1], pipeline=Pipeline("p1")
        )
        zipped = Zip[int](pipeline=Pipeline("p2"))

        with pytest.raises(RuntimeError):
            zipped.add_source(source)

        assert zipped.source_count() == 0

    async def test_rshift_adds_source_and_returns_zip(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        zipped = Zip[int]()

        result = source >> zipped

        assert result is zipped
        assert zipped.source_count() == 1

    async def test_rshift_sources_are_zipped_in_connection_order(self) -> None:
        first: IterableSource[int] = IterableSource(source=[1, 2])
        second: IterableSource[int] = IterableSource(source=[10, 20])
        zipped = first >> Zip[int, int]()
        _ = second >> zipped
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10), (2, 20)]

    async def test_rshift_accepts_zip_as_source_of_zip(self) -> None:
        inner = IterableSource(source=[5]) >> Zip[int]()
        outer = Zip[tuple[int]]()

        result = inner >> outer

        assert result is outer
        assert outer.source_count() == 1

    async def test_rshift_tuple_adds_sources_in_order(self) -> None:
        first: IterableSource[int] = IterableSource(source=[1, 2])
        second: IterableSource[int] = IterableSource(source=[10, 20])
        zipped = (first, second) >> Zip[int, int]()
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10), (2, 20)]

    async def test_lshift_adds_source_and_returns_zip(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        zipped = Zip[int]()

        result = zipped << source

        assert result is zipped
        assert zipped.source_count() == 1

    async def test_lshift_tuple_adds_sources_in_order(self) -> None:
        first: IterableSource[int] = IterableSource(source=[1, 2])
        second: IterableSource[int] = IterableSource(source=[10, 20])
        zipped = Zip[int, int]() << (first, second)
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10), (2, 20)]

    async def test_rshift_then_lshift_chains_sources(self) -> None:
        first: IterableSource[int] = IterableSource(source=[1, 2])
        second: IterableSource[int] = IterableSource(source=[10, 20])
        zipped = first >> Zip[int, int]() << second
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10), (2, 20)]

    async def test_lshift_tuple_from_different_pipeline_adds_nothing(self) -> None:
        local: IterableSource[int] = IterableSource(source=[1])
        foreign: IterableSource[int] = IterableSource(
            source=[2], pipeline=Pipeline("other")
        )
        zipped = Zip[int]()

        with pytest.raises(RuntimeError):
            _ = zipped << (local, foreign)

        assert zipped.source_count() == 0

    async def test_lshift_tuple_with_non_producer_raises_type_error(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        zipped = Zip[int]()

        with pytest.raises(TypeError):
            _ = zipped << (source, 5)  # type: ignore[operator]

        assert zipped.source_count() == 0

    async def test_rshift_from_different_pipeline_raises(self) -> None:
        source: IterableSource[int] = IterableSource(
            source=[1], pipeline=Pipeline("p1")
        )
        zipped = Zip[int](pipeline=Pipeline("p2"))

        with pytest.raises(RuntimeError):
            _ = source >> zipped

        assert zipped.source_count() == 0

    async def test_anext_without_sources_ends_stream(self) -> None:
        zipped = Zip[int]()
        await zipped.mount()

        with pytest.raises(StopAsyncIteration):
            await zipped.__anext__()

        assert zipped.state is ElementState.NULL

    async def test_yields_one_item_per_source_in_source_order(self) -> None:
        zipped = _zip_of(
            Zip[int, int, int](), [[1, 2, 3], [10, 20, 30], [100, 200, 300]]
        )
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10, 100), (2, 20, 200), (3, 30, 300)]
        assert zipped.state is ElementState.NULL

    async def test_yields_items_of_different_types_per_position(self) -> None:
        numbers: IterableSource[int] = IterableSource(source=[1, 2])
        letters: IterableSource[str] = IterableSource(source=["a", "b"])
        zipped = (numbers, letters) >> Zip[int, str]()
        await zipped.mount()

        result: list[tuple[int, str]] = [frame async for frame in zipped]

        assert result == [(1, "a"), (2, "b")]

    async def test_stops_when_any_source_is_exhausted(self) -> None:
        zipped = _zip_of(Zip[int, int](), [[1, 2, 3], [10]])
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10)]
        assert zipped.state is ElementState.NULL

    async def test_exhausted_as_none_fills_exhausted_sources(self) -> None:
        zipped = _zip_of(
            Zip[int | None, int | None](exhausted_as_none=True), [[1, 2, 3], [10]]
        )
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10), (2, None), (3, None)]
        assert zipped.state is ElementState.NULL

    async def test_exhausted_as_none_stops_when_all_sources_exhausted(self) -> None:
        zipped = _zip_of(
            Zip[int | None, int | None](exhausted_as_none=True), [[1], [10]]
        )
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, 10)]
        assert zipped.state is ElementState.NULL

    @pytest.mark.parametrize("exhausted_as_none", [False, True])
    async def test_source_exception_is_reraised(self, exhausted_as_none: bool) -> None:
        zipped = Zip[int | None, int | None](exhausted_as_none=exhausted_as_none)
        zipped.add_source(IterableSource(source=[1, 2, 3]))
        zipped.add_source(AsyncIterableSource(source=_failing_frames([10])))
        await zipped.mount()
        _ = await zipped.__anext__()

        with pytest.raises(ValueError, match="source failed"):
            await zipped.__anext__()

        assert zipped.state is ElementState.ERROR

    async def test_mount_after_end_restarts_from_all_sources(self) -> None:
        zipped = _zip_of(
            Zip[int | None, int | None](exhausted_as_none=True), [[1, 2], [10]]
        )
        await zipped.mount()
        _ = [frame async for frame in zipped]

        await zipped.mount()

        result = [frame async for frame in zipped]
        assert result == [(1, 10), (2, None)]


class TestZipSources:
    async def test_zips_sources_in_argument_order(self) -> None:
        numbers: IterableSource[int] = IterableSource(source=[1, 2])
        letters: IterableSource[str] = IterableSource(source=["a", "b"])
        zipped: Zip[int, str] = zip_sources(numbers, letters)
        await zipped.mount()

        result: list[tuple[int, str]] = [frame async for frame in zipped]

        assert result == [(1, "a"), (2, "b")]
        assert zipped.source_count() == 2

    async def test_exhausted_as_none_is_passed_to_zip(self) -> None:
        numbers: IterableSource[int] = IterableSource(source=[1, 2])
        letters: IterableSource[str] = IterableSource(source=["a"])
        zipped: Zip[int | None, str | None] = zip_sources(
            numbers, letters, exhausted_as_none=True
        )
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(1, "a"), (2, None)]
        assert zipped.exhausted_as_none is True

    def test_pipeline_defaults_to_first_source_pipeline(self) -> None:
        pipeline = Pipeline("sources")
        numbers: IterableSource[int] = IterableSource(source=[1], pipeline=pipeline)
        letters: IterableSource[str] = IterableSource(source=["a"], pipeline=pipeline)

        zipped = zip_sources(numbers, letters, name="zipped")

        assert zipped.pipeline is pipeline
        assert zipped.name == "zipped"
        assert zipped.source_count() == 2

    def test_sources_from_different_pipelines_raise(self) -> None:
        numbers: IterableSource[int] = IterableSource(
            source=[1], pipeline=Pipeline("p1")
        )
        letters: IterableSource[str] = IterableSource(
            source=["a"], pipeline=Pipeline("p2")
        )

        with pytest.raises(RuntimeError):
            _ = zip_sources(numbers, letters)

    async def test_more_than_five_sources_are_zipped(self) -> None:
        sources = [IterableSource(source=[i]) for i in range(6)]
        zipped = zip_sources(*sources)
        await zipped.mount()

        result = [frame async for frame in zipped]

        assert result == [(0, 1, 2, 3, 4, 5)]
        assert zipped.source_count() == 6


# ---------------------------------------------------------------------------
# TestBin
# ---------------------------------------------------------------------------


class Shout(Bin[str, str]):
    """Bin composing Strip and Upper into a new stage."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        strip = Strip[str]()
        self.set_upstream_consumer(strip)
        self.set_downstream_producer(strip >> Upper())


class TensAndTexts(Bin[int, tuple[int, str]]):
    """Bin with a fan-out graph inside: Tee into two maps, joined by Zip."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        tee = Tee[int]()
        self.set_upstream_consumer(tee)
        self.set_downstream_producer(
            tee
            >> (
                Map[int, int](map_func=lambda frame: frame * 10),
                Map[int, str](map_func=str),
            )
            >> Zip[int, str]()
        )


def _fail_on_two(frame: int) -> int:
    if frame == 2:
        raise ValueError("inner failure")
    return frame


class TestBin:
    async def test_subclass_composes_inner_stages(self) -> None:
        stage = IterableSource(source=[" hi ", " yo"]) >> Shout()
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["HI", "YO"]
        assert stage.state is ElementState.NULL

    async def test_upstream_and_downstream_keywords_compose_inner_stages(
        self,
    ) -> None:
        strip = Strip[str]()
        stage = IterableSource(source=[" a "]) >> Bin[str, str](
            upstream=strip, downstream=strip >> Upper()
        )
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["A"]

    async def test_inner_fan_out_graph_feeds_every_branch(self) -> None:
        sink = (
            IterableSource(source=[1, 2, 3])
            >> TensAndTexts()
            >> Last[tuple[int, str]]()
        )

        result = await sink.get_frame()

        assert result == (30, "3")

    async def test_mount_after_end_restarts_inner_stages(self) -> None:
        stage = IterableSource(source=[" hi "]) >> Shout()
        await stage.mount()
        _ = [frame async for frame in stage]
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["HI"]

    async def test_inner_exception_is_reraised(self) -> None:
        failing = Map[int, int](map_func=_fail_on_two)
        stage = IterableSource(source=[1, 2, 3]) >> Bin[int, int](
            upstream=failing, downstream=failing
        )
        await stage.mount()
        _ = await stage.__anext__()

        with pytest.raises(ValueError, match="inner failure"):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR

    async def test_mount_without_inner_elements_raises(self) -> None:
        stage = IterableSource(source=[1]) >> Bin[int, int]()

        with pytest.raises(RuntimeError, match="upstream consumer"):
            await stage.mount()

        assert stage.state is ElementState.ERROR

    def test_set_upstream_consumer_registers_and_connects_it(self) -> None:
        stage = Bin[str, str]()
        strip = Strip[str]()

        stage.set_upstream_consumer(strip)

        assert stage.upstream_consumer is strip
        assert strip.source is not None
        assert strip.source.pipeline is stage.pipeline

    def test_set_downstream_producer_registers_it(self) -> None:
        stage = Bin[str, str]()
        strip = Strip[str]()

        stage.set_downstream_producer(strip)

        assert stage.downstream_producer is strip

    def test_inner_element_from_different_pipeline_raises(self) -> None:
        stage = Bin[str, str](pipeline=Pipeline("bin"))
        strip = Strip[str](pipeline=Pipeline("other"))

        with pytest.raises(RuntimeError, match="pipeline"):
            stage.set_upstream_consumer(strip)

        assert stage.upstream_consumer is None

    async def test_changing_inner_elements_while_mounted_raises(self) -> None:
        stage = IterableSource(source=[" hi "]) >> Shout()
        await stage.mount()
        replacement = Strip[str]()

        with pytest.raises(RuntimeError, match="Unmount"):
            stage.set_downstream_producer(replacement)

        assert stage.downstream_producer is not replacement


# ---------------------------------------------------------------------------
# TestBaseBin
# ---------------------------------------------------------------------------


class Suffixed(BaseBin[str, str]):
    """BaseBin building its inner elements from an attribute set in __init__."""

    def __init__(self, *args: Any, suffix: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.suffix = suffix
        self.builds = 0

    def _get_upstream_downstream(self) -> tuple[BaseConsumer[str], BaseProducer[str]]:
        self.builds += 1
        strip = Strip[str]()
        suffix = self.suffix
        return strip, strip >> Map[str, str](map_func=lambda frame: frame + suffix)


class ForeignInner(BaseBin[str, str]):
    """BaseBin returning an inner element from another pipeline."""

    def _get_upstream_downstream(self) -> tuple[BaseConsumer[str], BaseProducer[str]]:
        strip = Strip[str](pipeline=Pipeline("other"))
        return strip, strip


class TestBaseBin:
    async def test_subclass_builds_inner_elements_from_init_attributes(
        self,
    ) -> None:
        stage = IterableSource(source=[" a ", " b"]) >> Suffixed(suffix="!")
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["a!", "b!"]
        assert stage.state is ElementState.NULL

    async def test_mount_builds_inner_elements(self) -> None:
        stage = IterableSource(source=[" a "]) >> Suffixed(suffix="!")
        assert stage.upstream_consumer is None
        assert stage.downstream_producer is None

        await stage.mount()

        assert stage.builds == 1
        assert isinstance(stage.upstream_consumer, Strip)
        assert stage.downstream_producer is not None
        assert stage.state is ElementState.READY

    async def test_remount_reuses_inner_elements(self) -> None:
        stage = IterableSource(source=[" a "]) >> Suffixed(suffix="!")
        await stage.mount()
        downstream = stage.downstream_producer
        _ = [frame async for frame in stage]

        await stage.mount()

        assert stage.builds == 1
        assert stage.downstream_producer is downstream
        assert stage.state is ElementState.READY

    async def test_inner_element_from_different_pipeline_raises_on_mount(
        self,
    ) -> None:
        stage = IterableSource(source=[" a "]) >> ForeignInner()

        with pytest.raises(RuntimeError, match="pipeline"):
            await stage.mount()

        assert stage.state is ElementState.ERROR

    def test_base_bin_cannot_be_instantiated(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            _ = BaseBin[str, str]()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# TestBinStageDecorator
# ---------------------------------------------------------------------------


@bin_stage(name="shout_bin")
def shout_bin() -> tuple[BaseConsumer[str], BaseProducer[str]]:
    """Strip and upper-case each frame."""
    strip = Strip[str]()
    return strip, strip >> Upper()


@bin_stage
def suffix_bin(*, suffix: str = "?") -> tuple[BaseConsumer[str], BaseProducer[str]]:
    strip = Strip[str]()
    return strip, strip >> Map[str, str](map_func=lambda frame: frame + suffix)


class TestBinStageDecorator:
    async def test_built_class_composes_builder_elements(self) -> None:
        stage = IterableSource(source=[" hi ", " yo"]) >> shout_bin()
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["HI", "YO"]
        assert isinstance(stage, BaseBin)

    def test_name_sets_class_name_and_element_prefix(self) -> None:
        stage = shout_bin()

        result = stage.name

        assert result.startswith("shout_bin-")
        assert shout_bin.__name__ == "shout_bin"
        assert shout_bin.__doc__ == "Strip and upper-case each frame."

    def test_name_defaults_to_builder_name(self) -> None:
        result = suffix_bin.__name__

        assert result == "suffix_bin"

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [({"suffix": "!"}, ["a!"]), ({}, ["a?"])],
        ids=["given", "default"],
    )
    async def test_builder_keywords_become_class_keywords(
        self, kwargs: dict[str, str], expected: list[str]
    ) -> None:
        stage = IterableSource(source=[" a "]) >> suffix_bin(**kwargs)
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == expected

    async def test_builder_elements_join_the_bin_pipeline(self) -> None:
        pipeline = Pipeline("own")
        stage = IterableSource(source=[" x "], pipeline=pipeline) >> shout_bin(
            pipeline=pipeline
        )

        await stage.mount()

        assert stage.upstream_consumer is not None
        assert stage.upstream_consumer.pipeline is pipeline
        assert stage.state is ElementState.READY

    async def test_each_instance_gets_its_own_inner_elements(self) -> None:
        first = IterableSource(source=[" a "]) >> shout_bin()
        second = IterableSource(source=[" b "]) >> shout_bin()
        await first.mount()

        await second.mount()

        assert first.upstream_consumer is not None
        assert first.upstream_consumer is not second.upstream_consumer


# ---------------------------------------------------------------------------
# TestPrefixStream / TestSuffixStream
# ---------------------------------------------------------------------------


class TestPrefixStream:
    @pytest.mark.parametrize(
        ("frames", "expected"),
        [(["a", "b"], ["[", "a", "b"]), ([], ["["])],
        ids=["frames", "empty"],
    )
    async def test_emits_prefix_before_frames(
        self, frames: list[str], expected: list[str]
    ) -> None:
        stage = IterableSource(source=frames) >> PrefixStream[str](prefix="[")
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == expected
        assert stage.state is ElementState.NULL

    async def test_mount_after_end_emits_prefix_again(self) -> None:
        stage = IterableSource(source=["a"]) >> PrefixStream[str](prefix="[")
        await stage.mount()
        _ = [frame async for frame in stage]
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["[", "a"]


class TestSuffixStream:
    @pytest.mark.parametrize(
        ("frames", "expected"),
        [(["a", "b"], ["a", "b", "]"]), ([], ["]"])],
        ids=["frames", "empty"],
    )
    async def test_emits_suffix_after_frames(
        self, frames: list[str], expected: list[str]
    ) -> None:
        stage = IterableSource(source=frames) >> SuffixStream[str](suffix="]")
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == expected
        assert stage.state is ElementState.NULL

    async def test_mount_after_end_emits_suffix_again(self) -> None:
        stage = IterableSource(source=["a"]) >> SuffixStream[str](suffix="]")
        await stage.mount()
        _ = [frame async for frame in stage]
        await stage.mount()

        result = [frame async for frame in stage]

        assert result == ["a", "]"]


# ---------------------------------------------------------------------------
# TestFlat
# ---------------------------------------------------------------------------


class TestFlat:
    async def test_flattens_single_inner_source(self) -> None:
        inner: IterableSource[int] = IterableSource(source=[1, 2, 3])
        outer: IterableSource[IterableSource[int]] = IterableSource(source=[inner])
        stage = outer >> Flat[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == [1, 2, 3]

    async def test_flattens_multiple_inner_sources_in_order(self) -> None:
        inner1: IterableSource[int] = IterableSource(source=[1, 2])
        inner2: IterableSource[int] = IterableSource(source=[3, 4])
        outer: IterableSource[IterableSource[int]] = IterableSource(
            source=[inner1, inner2]
        )
        stage = outer >> Flat[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == [1, 2, 3, 4]

    async def test_empty_inner_source_is_skipped(self) -> None:
        inner1: IterableSource[int] = IterableSource(source=[])
        inner2: IterableSource[int] = IterableSource(source=[1, 2])
        outer: IterableSource[IterableSource[int]] = IterableSource(
            source=[inner1, inner2]
        )
        stage = outer >> Flat[int]()
        await stage.mount()

        result = [item async for item in stage]

        assert result == [1, 2]

    async def test_last_sink_collects_all_flattened_frames(self) -> None:
        inner1: IterableSource[int] = IterableSource(source=[10, 20])
        inner2: IterableSource[int] = IterableSource(source=[30])
        outer: IterableSource[IterableSource[int]] = IterableSource(
            source=[inner1, inner2]
        )
        sink: Last[int] = outer >> Flat[int]() >> Last[int]()

        result = await sink.get_frame()

        assert result == 30


# ---------------------------------------------------------------------------
# TestRetry
# ---------------------------------------------------------------------------


class TestRetry:
    def test_init_sets_name_and_pipeline(self) -> None:
        pipeline = Pipeline("test")

        retry = Retry[int](name="retry", pipeline=pipeline, max_retries=2)

        assert retry.name == "retry"
        assert retry.pipeline is pipeline
        assert retry.state is ElementState.NULL


# ---------------------------------------------------------------------------
# TestGate
# ---------------------------------------------------------------------------


async def _is_pending(task: asyncio.Future[Any], within: float = 0.05) -> bool:
    done, _ = await asyncio.wait({task}, timeout=within)
    return not done


async def _cancel(task: asyncio.Future[Any]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


class TestGate:
    async def test_anext_blocks_while_gate_is_closed(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        gate = source >> Gate[int]()
        await gate.mount()
        task = asyncio.ensure_future(gate.__anext__())

        pending = await _is_pending(task)
        await _cancel(task)

        assert pending
        assert gate.state is ElementState.READY

    async def test_closed_gate_does_not_pull_from_its_source(self) -> None:
        pulled: list[int] = []

        def frames() -> Iterator[int]:
            for frame in (1, 2):
                pulled.append(frame)
                yield frame

        gate = IterableSource(source=frames()) >> Gate[int]()
        await gate.mount()
        task = asyncio.ensure_future(gate.__anext__())

        pending = await _is_pending(task)
        await _cancel(task)

        assert pending
        assert pulled == []

    async def test_open_releases_blocked_anext(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        gate = source >> Gate[int]()
        await gate.mount()
        task = asyncio.ensure_future(gate.__anext__())
        await _is_pending(task)

        gate.open()
        result = await asyncio.wait_for(task, timeout=1)

        assert result == 1

    async def test_open_gate_passes_every_frame_in_order(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        gate = source >> Gate[int]()
        gate.open()
        await gate.mount()

        result = [frame async for frame in gate]

        assert result == [1, 2, 3]
        assert gate.state is ElementState.NULL

    async def test_close_blocks_next_anext(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        gate = source >> Gate[int]()
        gate.open()
        await gate.mount()
        first = await gate.__anext__()
        gate.close()
        task = asyncio.ensure_future(gate.__anext__())

        pending = await _is_pending(task)
        await _cancel(task)

        assert first == 1
        assert pending

    async def test_reopen_resumes_frames(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        gate = source >> Gate[int]()
        gate.open()
        await gate.mount()
        _ = await gate.__anext__()
        gate.close()
        task = asyncio.ensure_future(gate.__anext__())
        await _is_pending(task)

        gate.open()
        result = await asyncio.wait_for(task, timeout=1)

        assert result == 2

    async def test_close_while_frame_is_in_flight_holds_the_frame(self) -> None:
        source: QueueSource[int] = QueueSource()
        gate = source >> Gate[int]()
        gate.open()
        await gate.mount()
        task = asyncio.ensure_future(gate.__anext__())
        await asyncio.sleep(0.01)  # let the gate start waiting on its source
        gate.close()
        await source.push_frame(7)

        pending = await _is_pending(task)
        await _cancel(task)

        assert pending

    async def test_open_releases_frame_held_in_flight(self) -> None:
        source: QueueSource[int] = QueueSource()
        gate = source >> Gate[int]()
        gate.open()
        await gate.mount()
        task = asyncio.ensure_future(gate.__anext__())
        await asyncio.sleep(0.01)  # let the gate start waiting on its source
        gate.close()
        await source.push_frame(7)
        await _is_pending(task)

        gate.open()
        result = await asyncio.wait_for(task, timeout=1)

        assert result == 7

    async def test_unmount_cancels_blocked_anext(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        gate = source >> Gate[int]()
        await gate.mount()
        task = asyncio.ensure_future(gate.__anext__())
        await _is_pending(task)

        await gate.unmount()
        await asyncio.wait({task}, timeout=1)

        assert task.cancelled()
        assert gate.state is ElementState.NULL

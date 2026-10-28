import asyncio
from contextlib import suppress

import pytest

from pyrill.base import BaseProducer, ElementState
from pyrill.bytes import BytesDataAccumulator
from pyrill.chunks import SkipUntil
from pyrill.sources import IterableSource
from pyrill.strings import StringDataAccumulator

_TIMEOUT: float = 1.0


async def _collect[T](producer: BaseProducer[T]) -> list[T]:
    await producer.mount()
    frames: list[T] = []
    async with asyncio.timeout(_TIMEOUT):
        with suppress(StopAsyncIteration):
            while True:
                frames.append(await producer.__anext__())
    return frames


# ---------------------------------------------------------------------------
# TestDataAccumulator
# ---------------------------------------------------------------------------


class TestDataAccumulator:
    async def test_emits_all_frames_joined_at_end_of_stream(self) -> None:
        stage: StringDataAccumulator = (
            IterableSource(source=["ab", "c", "de"]) >> StringDataAccumulator()
        )

        result = await _collect(stage)

        assert result == ["abcde"]
        assert stage.state is ElementState.NULL

    async def test_empty_stream_emits_nothing(self) -> None:
        stage: BytesDataAccumulator = (
            IterableSource[bytes](source=[]) >> BytesDataAccumulator()
        )

        result = await _collect(stage)

        assert result == []

    async def test_overflow_raises_when_buffer_exceeds_limit(self) -> None:
        stage: StringDataAccumulator = IterableSource(
            source=["ab", "cd", "ef"]
        ) >> StringDataAccumulator(max_buffer_size=4)
        await stage.mount()

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR


# ---------------------------------------------------------------------------
# TestSkipUntil
# ---------------------------------------------------------------------------


class TestSkipUntil:
    @pytest.mark.parametrize(
        ("frames", "expected"),
        [
            (["junk START payload", " tail"], [" payload", " tail"]),
            (["junk ST", "ART payload", " tail"], [" payload", " tail"]),
            (["S", "T", "A", "R", "T", "x"], ["", "x"]),
            (["no mark here", " at all"], []),
        ],
        ids=["mark-in-frame", "mark-split", "mark-one-char-per-frame", "no-mark"],
    )
    async def test_emits_data_after_mark(
        self, frames: list[str], expected: list[str]
    ) -> None:
        stage: SkipUntil[str] = IterableSource(source=frames) >> SkipUntil[str](
            mark="START"
        )

        result = await _collect(stage)

        assert result == expected
        assert stage.state is ElementState.NULL

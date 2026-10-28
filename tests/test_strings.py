import asyncio
from contextlib import suppress

import pytest

from pyrill.base import BaseProducer, ElementState
from pyrill.chunks import (
    BaseDataChunkProducer,
    BaseDataChunkSource,
    BaseDataChunkStage,
)
from pyrill.sources import IterableSource
from pyrill.strings import (
    Encode,
    Lower,
    StringChunksFirstSeparator,
    StringChunksFirstSeparatorSource,
    StringChunksSeparator,
    StringChunksSeparatorSource,
    StringChunksSlowStart,
    StringChunksSlowStartSource,
    StringSizedChunks,
    StringSizedChunksSource,
    Upper,
)

# Guards against stages that block forever instead of reading upstream.
_TIMEOUT: float = 1.0


async def _read_all[T](producer: BaseProducer[T]) -> list[T]:
    frames: list[T] = []
    async with asyncio.timeout(_TIMEOUT):
        with suppress(StopAsyncIteration):
            while True:
                frames.append(await producer.__anext__())
    return frames


async def _collect[T](producer: BaseProducer[T]) -> list[T]:
    await producer.mount()
    return await _read_all(producer)


async def _push_and_collect(
    source: BaseDataChunkSource[str], frames: list[str]
) -> list[str]:
    await source.mount()
    for frame in frames:
        await source.push_frame(frame)
    await source.end_of_stream()
    return await _read_all(source)


# ---------------------------------------------------------------------------
# TestStringSizedChunks
# ---------------------------------------------------------------------------


class TestStringSizedChunks:
    async def test_emits_chunks_no_larger_than_max_size(self) -> None:
        stage = IterableSource(source=["abcdefghij"]) >> StringSizedChunks(
            min_size=1, max_size=4
        )

        result = await _collect(stage)

        assert result == ["abcd", "efgh", "ij"]

    async def test_accumulates_frames_until_buffer_exceeds_min_size(self) -> None:
        stage = IterableSource(source=["ab", "cd", "ef"]) >> StringSizedChunks(
            min_size=3, max_size=10
        )
        await stage.mount()

        result = await stage.__anext__()

        assert result == "abcd"

    async def test_flushes_remainder_smaller_than_min_size_at_end_of_stream(
        self,
    ) -> None:
        stage = IterableSource(source=["abcde"]) >> StringSizedChunks(
            min_size=3, max_size=4
        )

        result = await _collect(stage)

        assert result == ["abcd", "e"]

    async def test_sizes_are_measured_in_characters_not_bytes(self) -> None:
        stage = IterableSource(source=["café", "ñ"]) >> StringSizedChunks(
            min_size=1, max_size=3
        )

        result = await _collect(stage)

        assert result == ["caf", "éñ"]


# ---------------------------------------------------------------------------
# TestStringSizedChunksSource
# ---------------------------------------------------------------------------


class TestStringSizedChunksSource:
    async def test_emits_chunks_from_pushed_frames(self) -> None:
        source = StringSizedChunksSource(min_size=1, max_size=4)

        result = await _push_and_collect(source, ["abcdefghij"])

        assert result == ["abcd", "efgh", "ij"]


# ---------------------------------------------------------------------------
# TestStringChunksSlowStart
# ---------------------------------------------------------------------------


class TestStringChunksSlowStart:
    async def test_fast_frames_are_merged_as_threshold_grows(self) -> None:
        stage = IterableSource(source=["abcd", "efgh", "ijkl", "mnop"]) >> (
            StringChunksSlowStart(min_size=4, max_size=64)
        )

        result = await _collect(stage)

        assert result == ["abcdefghijklmnop"]

    async def test_first_chunk_is_sized_by_initial_min_size(self) -> None:
        stage = IterableSource(source=["abcdef"]) >> StringChunksSlowStart(
            min_size=1, max_size=4
        )
        await stage.mount()

        result = await stage.__anext__()

        assert result == "abcd"


# ---------------------------------------------------------------------------
# TestStringChunksSlowStartSource
# ---------------------------------------------------------------------------


class TestStringChunksSlowStartSource:
    async def test_emits_chunks_from_pushed_frames(self) -> None:
        source = StringChunksSlowStartSource(min_size=1, max_size=4)

        result = await _push_and_collect(source, ["abcdef"])

        assert result == ["abcd", "ef"]


# ---------------------------------------------------------------------------
# TestStringChunksSeparator
# ---------------------------------------------------------------------------


class TestStringChunksSeparator:
    async def test_splits_on_newline_by_default(self) -> None:
        stage = IterableSource(source=["a\nb", "c\n"]) >> StringChunksSeparator()

        result = await _collect(stage)

        assert result == ["a\n", "bc\n"]

    async def test_splits_on_custom_separator(self) -> None:
        stage = IterableSource(source=["a;b;"]) >> StringChunksSeparator(separator=";")

        result = await _collect(stage)

        assert result == ["a;", "b;"]

    async def test_flushes_remainder_without_separator_at_end_of_stream(
        self,
    ) -> None:
        stage = IterableSource(source=["a\nbc"]) >> StringChunksSeparator()

        result = await _collect(stage)

        assert result == ["a\n", "bc"]


# ---------------------------------------------------------------------------
# TestStringChunksSeparatorSource
# ---------------------------------------------------------------------------


class TestStringChunksSeparatorSource:
    async def test_splits_pushed_frames_on_newline(self) -> None:
        source = StringChunksSeparatorSource()

        result = await _push_and_collect(source, ["a\nb", "c\n"])

        assert result == ["a\n", "bc\n"]

    async def test_flushes_remainder_without_separator_at_end_of_stream(
        self,
    ) -> None:
        source = StringChunksSeparatorSource()

        result = await _push_and_collect(source, ["a\nb", "c\nd"])

        assert result == ["a\n", "bc\n", "d"]

    async def test_push_frame_before_mount_raises_runtime_error(self) -> None:
        source = StringChunksSeparatorSource()

        with pytest.raises(RuntimeError, match="not mounted"):
            await source.push_frame("a\n")


# ---------------------------------------------------------------------------
# TestStringChunksFirstSeparator
# ---------------------------------------------------------------------------


class TestStringChunksFirstSeparator:
    async def test_first_chunk_ends_at_first_separator(self) -> None:
        stage = IterableSource(source=["head", "er\nbody"]) >> (
            StringChunksFirstSeparator()
        )
        await stage.mount()

        async with asyncio.timeout(_TIMEOUT):
            result = await stage.__anext__()

        assert result == "header\n"

    async def test_data_after_first_separator_is_passed_through(self) -> None:
        stage = IterableSource(source=["head", "er\nbody", " more"]) >> (
            StringChunksFirstSeparator()
        )

        result = await _collect(stage)

        assert result == ["header\n", "body", " more"]

    async def test_flushes_input_without_separator_at_end_of_stream(self) -> None:
        stage = IterableSource(source=["abc"]) >> StringChunksFirstSeparator()

        result = await _collect(stage)

        assert result == ["abc"]


# ---------------------------------------------------------------------------
# TestStringChunksFirstSeparatorSource
# ---------------------------------------------------------------------------


class TestStringChunksFirstSeparatorSource:
    async def test_first_chunk_ends_at_first_separator(self) -> None:
        source = StringChunksFirstSeparatorSource()

        result = await _push_and_collect(source, ["head", "er\nbody"])

        assert result == ["header\n", "body"]

    async def test_flushes_input_without_separator_at_end_of_stream(self) -> None:
        source = StringChunksFirstSeparatorSource()

        result = await _push_and_collect(source, ["abc"])

        assert result == ["abc"]


# ---------------------------------------------------------------------------
# TestLower / TestUpper
# ---------------------------------------------------------------------------


class TestLower:
    async def test_lowercases_each_frame(self) -> None:
        stage = IterableSource(source=["HeLLo", "ÀÉ"]) >> Lower()

        result = await _collect(stage)

        assert result == ["hello", "àé"]


class TestUpper:
    async def test_uppercases_each_frame(self) -> None:
        stage = IterableSource(source=["hello", "àé"]) >> Upper()

        result = await _collect(stage)

        assert result == ["HELLO", "ÀÉ"]

    async def test_applies_full_unicode_case_mapping(self) -> None:
        stage = IterableSource(source=["straße"]) >> Upper()

        result = await _collect(stage)

        assert result == ["STRASSE"]


# ---------------------------------------------------------------------------
# TestEncode
# ---------------------------------------------------------------------------


class TestEncode:
    async def test_encodes_utf8_by_default(self) -> None:
        stage = IterableSource(source=["café"]) >> Encode()

        result = await _collect(stage)

        assert result == [b"caf\xc3\xa9"]

    async def test_encodes_with_configured_encoding(self) -> None:
        stage = IterableSource(source=["café"]) >> Encode(encoding="latin-1")

        result = await _collect(stage)

        assert result == [b"caf\xe9"]

    async def test_unencodable_character_raises_by_default(self) -> None:
        stage = IterableSource(source=["café"]) >> Encode(encoding="ascii")
        await stage.mount()

        with pytest.raises(UnicodeEncodeError):
            await stage.__anext__()

    @pytest.mark.parametrize(
        ("errors", "expected"),
        [
            pytest.param("replace", b"caf?", id="replace"),
            pytest.param("ignore", b"caf", id="ignore"),
        ],
    )
    async def test_errors_policy_handles_unencodable_character(
        self, errors: str, expected: bytes
    ) -> None:
        stage = IterableSource(source=["café"]) >> Encode(
            encoding="ascii", errors=errors
        )

        result = await _collect(stage)

        assert result == [expected]


# ---------------------------------------------------------------------------
# TestMaxBufferSize
# ---------------------------------------------------------------------------

_LIMIT: int = 4

# Frames chosen so that no stage can emit before 5 characters are buffered:
# min_size is above the total, or there is no separator.
_STAGES: list[object] = [
    pytest.param(StringSizedChunks, {"min_size": 10}, id="StringSizedChunks"),
    pytest.param(StringChunksSlowStart, {"min_size": 10}, id="StringChunksSlowStart"),
    pytest.param(StringChunksSeparator, {}, id="StringChunksSeparator"),
    pytest.param(StringChunksFirstSeparator, {}, id="StringChunksFirstSeparator"),
]

_SOURCES: list[object] = [
    pytest.param(StringSizedChunksSource, {}, id="StringSizedChunksSource"),
    pytest.param(StringChunksSlowStartSource, {}, id="StringChunksSlowStartSource"),
    pytest.param(StringChunksSeparatorSource, {}, id="StringChunksSeparatorSource"),
    pytest.param(
        StringChunksFirstSeparatorSource, {}, id="StringChunksFirstSeparatorSource"
    ),
]


def _with_limit[T: BaseDataChunkProducer[str]](
    element_cls: type[T], kwargs: dict[str, int]
) -> T:
    return element_cls(max_buffer_size=_LIMIT, **kwargs)


async def _overflowed_source(
    source_cls: type[BaseDataChunkSource[str]], kwargs: dict[str, int]
) -> BaseDataChunkSource[str]:
    source = _with_limit(source_cls, kwargs)
    await source.mount()
    await source.push_frame("abc")
    with suppress(OverflowError):
        await source.push_frame("de")
    return source


class TestMaxBufferSizeOnStages:
    @pytest.mark.parametrize(("stage_cls", "kwargs"), _STAGES)
    async def test_raises_overflow_error_when_buffer_exceeds_limit(
        self, stage_cls: type[BaseDataChunkStage[str]], kwargs: dict[str, int]
    ) -> None:
        stage = IterableSource(source=["abc", "de"]) >> _with_limit(stage_cls, kwargs)
        await stage.mount()

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await stage.__anext__()

    @pytest.mark.parametrize(("stage_cls", "kwargs"), _STAGES)
    async def test_buffer_exactly_at_limit_is_accepted(
        self, stage_cls: type[BaseDataChunkStage[str]], kwargs: dict[str, int]
    ) -> None:
        stage = IterableSource(source=["ab", "cd"]) >> _with_limit(stage_cls, kwargs)

        result = await _collect(stage)

        assert result == ["abcd"]

    async def test_limit_counts_characters_not_bytes(self) -> None:
        # "éééé" is 4 characters but 8 UTF-8 bytes.
        stage = IterableSource(source=["éé", "éé"]) >> StringChunksSeparator(
            max_buffer_size=_LIMIT
        )

        result = await _collect(stage)

        assert result == ["éééé"]


class TestMaxBufferSizeOnSources:
    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_push_frame_raises_overflow_error_when_buffer_exceeds_limit(
        self, source_cls: type[BaseDataChunkSource[str]], kwargs: dict[str, int]
    ) -> None:
        source = _with_limit(source_cls, kwargs)
        await source.mount()
        await source.push_frame("abc")

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await source.push_frame("de")

    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_push_frame_after_overflow_raises_runtime_error(
        self, source_cls: type[BaseDataChunkSource[str]], kwargs: dict[str, int]
    ) -> None:
        source = await _overflowed_source(source_cls, kwargs)

        with pytest.raises(RuntimeError, match="already finished"):
            await source.push_frame("f")

    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_first_read_after_overflow_raises_without_emitting_buffered_data(
        self, source_cls: type[BaseDataChunkSource[str]], kwargs: dict[str, int]
    ) -> None:
        source = await _overflowed_source(source_cls, kwargs)

        with pytest.raises(OverflowError, match="max_buffer_size"):
            async with asyncio.timeout(_TIMEOUT):
                await source.__anext__()

        assert source.state is ElementState.ERROR

    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_push_frame_accepts_buffer_exactly_at_limit(
        self, source_cls: type[BaseDataChunkSource[str]], kwargs: dict[str, int]
    ) -> None:
        source = _with_limit(source_cls, kwargs)

        result = await _push_and_collect(source, ["ab", "cd"])

        assert result == ["abcd"]


class TestMaxBufferSizeDefault:
    @pytest.mark.parametrize(
        "element_cls",
        [
            StringSizedChunks,
            StringChunksSlowStart,
            StringChunksSeparator,
            StringChunksFirstSeparator,
            StringSizedChunksSource,
            StringChunksSlowStartSource,
            StringChunksSeparatorSource,
            StringChunksFirstSeparatorSource,
        ],
    )
    async def test_max_buffer_size_defaults_to_zero_disabled(
        self, element_cls: type[BaseDataChunkProducer[str]]
    ) -> None:
        element = element_cls()

        result = element.max_buffer_size

        assert result == 0

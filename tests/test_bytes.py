import asyncio
from contextlib import suppress

import pytest

from pyrill.base import BaseProducer, ElementState
from pyrill.bytes import (
    BytesChunksFirstSeparator,
    BytesChunksFirstSeparatorSource,
    BytesChunksSeparator,
    BytesChunksSeparatorSource,
    BytesChunksSlowStart,
    BytesChunksSlowStartSource,
    BytesSizedChunks,
    BytesSizedChunksSource,
    Decode,
    UnicodeChunks,
)
from pyrill.chunks import (
    BaseDataChunkProducer,
    BaseDataChunkSource,
    BaseDataChunkStage,
)
from pyrill.sources import IterableSource

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
    source: BaseDataChunkSource[bytes], frames: list[bytes]
) -> list[bytes]:
    await source.mount()
    for frame in frames:
        await source.push_frame(frame)
    await source.end_of_stream()
    return await _read_all(source)


# ---------------------------------------------------------------------------
# TestBytesSizedChunks
# ---------------------------------------------------------------------------


class TestBytesSizedChunks:
    async def test_emits_chunks_no_larger_than_max_size(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"abcdefghij"])
        stage: BytesSizedChunks = source >> BytesSizedChunks(min_size=1, max_size=4)

        result = await _collect(stage)

        assert result == [b"abcd", b"efgh", b"ij"]

    async def test_accumulates_frames_until_buffer_exceeds_min_size(self) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"ab", b"cd", b"ef"])
        stage: BytesSizedChunks = source >> BytesSizedChunks(min_size=3, max_size=10)
        await stage.mount()

        result = await stage.__anext__()

        assert result == b"abcd"

    async def test_flushes_remainder_smaller_than_min_size_at_end_of_stream(
        self,
    ) -> None:
        source: IterableSource[bytes] = IterableSource(source=[b"abcde"])
        stage: BytesSizedChunks = source >> BytesSizedChunks(min_size=3, max_size=4)

        result = await _collect(stage)

        assert result == [b"abcd", b"e"]


# ---------------------------------------------------------------------------
# TestBytesSizedChunksSource
# ---------------------------------------------------------------------------


class TestBytesSizedChunksSource:
    async def test_emits_chunks_from_pushed_frames(self) -> None:
        source = BytesSizedChunksSource(min_size=1, max_size=4)

        result = await _push_and_collect(source, [b"abcdefghij"])

        assert result == [b"abcd", b"efgh", b"ij"]


# ---------------------------------------------------------------------------
# TestBytesChunksSlowStart
# ---------------------------------------------------------------------------


class TestBytesChunksSlowStart:
    async def test_first_chunk_is_sized_by_initial_min_size(self) -> None:
        stage: BytesChunksSlowStart = IterableSource(
            source=[b"abcdef"]
        ) >> BytesChunksSlowStart(min_size=1, max_size=4)
        await stage.mount()

        result = await stage.__anext__()

        assert result == b"abcd"

    async def test_flushes_remainder_at_end_of_stream(self) -> None:
        stage: BytesChunksSlowStart = IterableSource(
            source=[b"abcdef"]
        ) >> BytesChunksSlowStart(min_size=1, max_size=4)

        result = await _collect(stage)

        assert result == [b"abcd", b"ef"]


# ---------------------------------------------------------------------------
# TestBytesChunksSlowStartSource
# ---------------------------------------------------------------------------


class TestBytesChunksSlowStartSource:
    async def test_emits_chunks_from_pushed_frames(self) -> None:
        source = BytesChunksSlowStartSource(min_size=1, max_size=4)

        result = await _push_and_collect(source, [b"abcdef"])

        assert result == [b"abcd", b"ef"]


# ---------------------------------------------------------------------------
# TestBytesChunksSeparator
# ---------------------------------------------------------------------------


class TestBytesChunksSeparator:
    async def test_splits_on_newline_by_default(self) -> None:
        stage: BytesChunksSeparator = (
            IterableSource(source=[b"a\nb", b"c\n"]) >> BytesChunksSeparator()
        )

        result = await _collect(stage)

        assert result == [b"a\n", b"bc\n"]

    async def test_splits_on_custom_separator(self) -> None:
        stage: BytesChunksSeparator = IterableSource(
            source=[b"a;b;"]
        ) >> BytesChunksSeparator(separator=b";")

        result = await _collect(stage)

        assert result == [b"a;", b"b;"]

    async def test_flushes_remainder_without_separator_at_end_of_stream(
        self,
    ) -> None:
        stage: BytesChunksSeparator = (
            IterableSource(source=[b"a\nbc"]) >> BytesChunksSeparator()
        )

        result = await _collect(stage)

        assert result == [b"a\n", b"bc"]


# ---------------------------------------------------------------------------
# TestBytesChunksSeparatorSource
# ---------------------------------------------------------------------------


class TestBytesChunksSeparatorSource:
    async def test_splits_pushed_frames_on_newline(self) -> None:
        source: BytesChunksSeparatorSource = BytesChunksSeparatorSource()

        result = await _push_and_collect(source, [b"a\nb", b"c\n"])

        assert result == [b"a\n", b"bc\n"]

    async def test_flushes_remainder_without_separator_at_end_of_stream(
        self,
    ) -> None:
        source: BytesChunksSeparatorSource = BytesChunksSeparatorSource()

        result = await _push_and_collect(source, [b"a\nbc"])

        assert result == [b"a\n", b"bc"]

    async def test_push_frame_before_mount_raises_runtime_error(self) -> None:
        source: BytesChunksSeparatorSource = BytesChunksSeparatorSource()

        with pytest.raises(RuntimeError, match="not mounted"):
            await source.push_frame(b"a\n")


# ---------------------------------------------------------------------------
# TestBytesChunksFirstSeparator
# ---------------------------------------------------------------------------


class TestBytesChunksFirstSeparator:
    async def test_first_chunk_ends_at_first_separator(self) -> None:
        stage: BytesChunksFirstSeparator = (
            IterableSource(source=[b"head", b"er\nbody"]) >> BytesChunksFirstSeparator()
        )
        await stage.mount()

        async with asyncio.timeout(_TIMEOUT):
            result = await stage.__anext__()

        assert result == b"header\n"

    async def test_data_after_first_separator_is_not_split(self) -> None:
        stage: BytesChunksFirstSeparator = (
            IterableSource(source=[b"a\nb\nc"]) >> BytesChunksFirstSeparator()
        )

        result = await _collect(stage)

        assert result == [b"a\n", b"b\nc"]


# ---------------------------------------------------------------------------
# TestBytesChunksFirstSeparatorSource
# ---------------------------------------------------------------------------


class TestBytesChunksFirstSeparatorSource:
    async def test_first_chunk_ends_at_first_separator(self) -> None:
        source: BytesChunksFirstSeparatorSource = BytesChunksFirstSeparatorSource()

        result = await _push_and_collect(source, [b"head", b"er\nbody"])

        assert result == [b"header\n", b"body"]

    async def test_flushes_input_without_separator_at_end_of_stream(self) -> None:
        source: BytesChunksFirstSeparatorSource = BytesChunksFirstSeparatorSource()

        result = await _push_and_collect(source, [b"abc"])

        assert result == [b"abc"]


# ---------------------------------------------------------------------------
# TestDecode
# ---------------------------------------------------------------------------


class TestDecode:
    async def test_decodes_utf8_by_default(self) -> None:
        stage = IterableSource(source=[b"caf\xc3\xa9"]) >> Decode()

        result = await _collect(stage)

        assert result == ["café"]

    async def test_decodes_with_configured_encoding(self) -> None:
        stage = IterableSource(source=[b"caf\xe9"]) >> Decode(encoding="latin-1")

        result = await _collect(stage)

        assert result == ["café"]

    async def test_invalid_bytes_raise_unicode_decode_error(self) -> None:
        stage = IterableSource(source=[b"\xff"]) >> Decode()
        await stage.mount()

        with pytest.raises(UnicodeDecodeError):
            await stage.__anext__()


# ---------------------------------------------------------------------------
# TestUnicodeChunks
# ---------------------------------------------------------------------------


class TestUnicodeChunks:
    async def test_invalid_byte_raises_after_valid_prefix(self) -> None:
        stage: UnicodeChunks = (
            IterableSource(source=[b"ok\xffgood", b" more"]) >> UnicodeChunks()
        )
        await stage.mount()
        assert await stage.__anext__() == b"ok"

        with pytest.raises(UnicodeDecodeError):
            await stage.__anext__()

        assert stage.state is ElementState.ERROR

    async def test_holds_incomplete_multibyte_sequence_until_complete(self) -> None:
        stage: UnicodeChunks = (
            IterableSource(source=[b"a\xc3", b"\xa9b"]) >> UnicodeChunks()
        )

        result = await _collect(stage)

        assert result == [b"a", b"\xc3\xa9b"]

    async def test_emits_complete_frame_unchanged(self) -> None:
        stage: UnicodeChunks = IterableSource(source=[b"abc"]) >> UnicodeChunks()

        result = await _collect(stage)

        assert result == [b"abc"]

    async def test_uses_configured_encoding(self) -> None:
        stage: UnicodeChunks = IterableSource(source=[b"\xe9"]) >> UnicodeChunks(
            encoding="latin-1"
        )

        result = await _collect(stage)

        assert result == [b"\xe9"]

    async def test_incomplete_trailing_sequence_ends_stream_instead_of_emitting_empty_chunks(
        self,
    ) -> None:
        stage: UnicodeChunks = IterableSource(source=[b"ok\xc3"]) >> UnicodeChunks()
        await stage.mount()
        assert await stage.__anext__() == b"ok"  # the decodable prefix

        with pytest.raises((StopAsyncIteration, UnicodeDecodeError)):
            print(await stage.__anext__())


# ---------------------------------------------------------------------------
# TestMaxBufferSize
# ---------------------------------------------------------------------------

_LIMIT: int = 4

# Frames chosen so that no stage can emit before 5 bytes are buffered:
# min_size is above the total, there is no separator, or the bytes are not
# decodable.
_OVERFLOWING_STAGES: list[object] = [
    pytest.param(
        BytesSizedChunks,
        {"min_size": 10},
        [b"abc", b"de"],
        id="BytesSizedChunks",
    ),
    pytest.param(
        BytesChunksSlowStart,
        {"min_size": 10},
        [b"abc", b"de"],
        id="BytesChunksSlowStart",
    ),
    pytest.param(
        BytesChunksSeparator,
        {},
        [b"abc", b"de"],
        id="BytesChunksSeparator",
    ),
    pytest.param(
        BytesChunksFirstSeparator,
        {},
        [b"abc", b"de"],
        id="BytesChunksFirstSeparator",
    ),
    pytest.param(
        UnicodeChunks,
        {},
        [b"\xe2\x82", b"\xac\xe2\x82\xac"],
        id="UnicodeChunks",
    ),
]

_BOUNDED_STAGES: list[object] = [
    pytest.param(
        BytesSizedChunks,
        {"min_size": 10},
        id="BytesSizedChunks",
    ),
    pytest.param(
        BytesChunksSlowStart,
        {"min_size": 10},
        id="BytesChunksSlowStart",
    ),
    pytest.param(
        BytesChunksSeparator,
        {},
        id="BytesChunksSeparator",
    ),
    pytest.param(
        BytesChunksFirstSeparator,
        {},
        id="BytesChunksFirstSeparator",
    ),
]

_SOURCES: list[object] = [
    pytest.param(
        BytesSizedChunksSource,
        {},
        id="BytesSizedChunksSource",
    ),
    pytest.param(
        BytesChunksSlowStartSource,
        {},
        id="BytesChunksSlowStartSource",
    ),
    pytest.param(
        BytesChunksSeparatorSource,
        {},
        id="BytesChunksSeparatorSource",
    ),
    pytest.param(
        BytesChunksFirstSeparatorSource,
        {},
        id="BytesChunksFirstSeparatorSource",
    ),
]


def _with_limit[T: BaseDataChunkProducer[bytes]](
    element_cls: type[T], kwargs: dict[str, int]
) -> T:
    return element_cls(max_buffer_size=_LIMIT, **kwargs)


async def _overflowed_source(
    source_cls: type[BaseDataChunkSource[bytes]], kwargs: dict[str, int]
) -> BaseDataChunkSource[bytes]:
    source = _with_limit(source_cls, kwargs)
    await source.mount()
    await source.push_frame(b"abc")
    with suppress(OverflowError):
        await source.push_frame(b"de")
    return source


class TestMaxBufferSizeOnStages:
    @pytest.mark.parametrize(("stage_cls", "kwargs", "frames"), _OVERFLOWING_STAGES)
    async def test_raises_overflow_error_when_buffer_exceeds_limit(
        self,
        stage_cls: type[BaseDataChunkStage[bytes]],
        kwargs: dict[str, int],
        frames: list[bytes],
    ) -> None:
        stage = IterableSource(source=frames) >> _with_limit(stage_cls, kwargs)
        await stage.mount()

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await stage.__anext__()

    @pytest.mark.parametrize(("stage_cls", "kwargs"), _BOUNDED_STAGES)
    async def test_buffer_exactly_at_limit_is_accepted(
        self, stage_cls: type[BaseDataChunkStage[bytes]], kwargs: dict[str, int]
    ) -> None:
        stage = IterableSource(source=[b"ab", b"cd"]) >> _with_limit(stage_cls, kwargs)

        result = await _collect(stage)

        assert result == [b"abcd"]

    async def test_single_frame_larger_than_limit_overflows_even_if_splittable(
        self,
    ) -> None:
        stage = IterableSource(source=[b"abcdef"]) >> BytesSizedChunks(
            min_size=1, max_size=2, max_buffer_size=4
        )
        await stage.mount()

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await stage.__anext__()

    @pytest.mark.parametrize(
        "stage",
        [
            pytest.param(BytesSizedChunks(min_size=10_000), id="BytesSizedChunks"),
            pytest.param(BytesChunksSeparator(), id="BytesChunksSeparator"),
        ],
    )
    async def test_default_limit_does_not_bound_buffer(
        self, stage: BaseDataChunkStage[bytes]
    ) -> None:
        pipeline = IterableSource(source=[b"a" * 5_000, b"a" * 5_000]) >> stage

        result = await _collect(pipeline)

        assert result == [b"a" * 10_000]


class TestMaxBufferSizeOnSources:
    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_push_frame_raises_overflow_error_when_buffer_exceeds_limit(
        self, source_cls: type[BaseDataChunkSource[bytes]], kwargs: dict[str, int]
    ) -> None:
        source = _with_limit(source_cls, kwargs)
        await source.mount()
        await source.push_frame(b"abc")

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await source.push_frame(b"de")

    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_push_frame_after_overflow_raises_runtime_error(
        self, source_cls: type[BaseDataChunkSource[bytes]], kwargs: dict[str, int]
    ) -> None:
        source = await _overflowed_source(source_cls, kwargs)

        with pytest.raises(RuntimeError, match="already finished"):
            await source.push_frame(b"f")

    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_first_read_after_overflow_raises_without_emitting_buffered_data(
        self, source_cls: type[BaseDataChunkSource[bytes]], kwargs: dict[str, int]
    ) -> None:
        source = await _overflowed_source(source_cls, kwargs)

        with pytest.raises(OverflowError, match="max_buffer_size"):
            async with asyncio.timeout(_TIMEOUT):
                await source.__anext__()

        assert source.state is ElementState.ERROR

    async def test_single_frame_larger_than_limit_overflows_even_if_splittable(
        self,
    ) -> None:
        source = BytesSizedChunksSource(min_size=1, max_size=2, max_buffer_size=4)
        await source.mount()

        with pytest.raises(OverflowError, match="max_buffer_size"):
            await source.push_frame(b"abcdef")

    @pytest.mark.parametrize(("source_cls", "kwargs"), _SOURCES)
    async def test_push_frame_accepts_buffer_exactly_at_limit(
        self, source_cls: type[BaseDataChunkSource[bytes]], kwargs: dict[str, int]
    ) -> None:
        source = _with_limit(source_cls, kwargs)

        result = await _push_and_collect(source, [b"ab", b"cd"])

        assert result == [b"abcd"]


class TestMaxBufferSizeDefault:
    @pytest.mark.parametrize(
        "element_cls",
        [
            BytesSizedChunks,
            BytesChunksSlowStart,
            BytesChunksSeparator,
            BytesChunksFirstSeparator,
            UnicodeChunks,
            BytesSizedChunksSource,
            BytesChunksSlowStartSource,
            BytesChunksSeparatorSource,
            BytesChunksFirstSeparatorSource,
        ],
    )
    async def test_max_buffer_size_defaults_to_zero_disabled(
        self, element_cls: type[BaseDataChunkProducer[bytes]]
    ) -> None:
        element = element_cls()

        result = element.max_buffer_size

        assert result == 0

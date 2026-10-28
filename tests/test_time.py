import asyncio
from collections.abc import Awaitable, Callable, Iterator
from contextlib import suppress
from datetime import UTC, datetime, timedelta, tzinfo
from unittest.mock import AsyncMock, call, patch

import pytest

from pyrill.base import BaseProducer, ElementState
from pyrill.queues import QueueSource
from pyrill.sources import IterableSource
from pyrill.time import Delayed, StartAt, StopAt, Timeout


async def _collect[T](producer: BaseProducer[T]) -> list[T]:
    await producer.mount()
    return [frame async for frame in producer]


async def _settle[T](awaitable: Awaitable[T], within: float = 1.0) -> asyncio.Future[T]:
    """Run ``awaitable`` for at most ``within`` seconds, cancelling it if still pending."""
    task = asyncio.ensure_future(awaitable)
    await asyncio.wait({task}, timeout=within)
    if not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    return task


def _clock(
    *readings: datetime, then: datetime
) -> Callable[[tzinfo | None], datetime]:
    """Fake ``datetime.now``: returns ``readings`` in order, then ``then`` forever."""
    remaining = iter(readings)

    def now(tz: tzinfo | None = None) -> datetime:
        return next(remaining, then)

    return now


# ---------------------------------------------------------------------------
# TestDelayed
# ---------------------------------------------------------------------------


class TestDelayed:
    def test_init_sets_delay(self) -> None:
        stage = Delayed[int](delay=0.5)

        assert stage.delay == 0.5
        assert stage.state is ElementState.NULL

    async def test_sleeps_delay_before_each_frame(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> Delayed[int](delay=0.5)

        with patch("pyrill.time.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await _collect(stage)

        assert result == [1, 2, 3]
        assert mock_sleep.await_args_list == [call(0.5)] * 3
        assert stage.state is ElementState.NULL

    async def test_empty_stream_does_not_sleep(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])
        stage = source >> Delayed[int](delay=0.5)

        with patch("pyrill.time.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await _collect(stage)

        assert result == []
        mock_sleep.assert_not_awaited()

    async def test_frame_is_not_emitted_before_the_delay_has_passed(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        stage = source >> Delayed[int](delay=0.05)
        await stage.mount()
        loop = asyncio.get_running_loop()
        started = loop.time()

        result = await stage.__anext__()

        assert result == 1
        assert loop.time() - started >= 0.04


# ---------------------------------------------------------------------------
# TestTimeout
# ---------------------------------------------------------------------------


class TestTimeout:
    def test_init_sets_timeout(self) -> None:
        stage = Timeout[int](timeout=2.5)

        assert stage.timeout == 2.5
        assert stage.state is ElementState.NULL

    async def test_frame_arriving_before_timeout_passes_through(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> Timeout[int](timeout=1)
        await stage.mount()
        await source.push_frame(42)

        result = await stage.__anext__()

        assert result == 42
        assert stage.state is ElementState.READY

    async def test_stream_end_is_not_a_timeout(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1])
        stage = source >> Timeout[int](timeout=1)

        result = await _collect(stage)

        assert result == [1]
        assert stage.state is ElementState.NULL

    async def test_raises_timeout_error_when_source_is_slower_than_timeout(
        self,
    ) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> Timeout[int](timeout=0.05)
        await stage.mount()

        outcome = await _settle(stage.__anext__())

        assert outcome.done()
        assert not outcome.cancelled()
        assert isinstance(outcome.exception(), TimeoutError)
        assert stage.state is ElementState.ERROR

    async def test_timeout_applies_to_each_frame_not_to_the_whole_stream(
        self,
    ) -> None:
        # Eight frames of 0.03s take 0.24s in total, more than the 0.2s timeout,
        # but every single wait is well within it.
        source: IterableSource[int] = IterableSource(source=list(range(8)))
        stage = source >> Delayed[int](delay=0.03) >> Timeout[int](timeout=0.2)

        result = await _collect(stage)

        assert result == list(range(8))
        assert stage.state is ElementState.NULL


# ---------------------------------------------------------------------------
# TestStopAt
# ---------------------------------------------------------------------------


class TestStopAt:
    def test_init_sets_stop_at(self) -> None:
        stop_at = datetime(2030, 1, 1, tzinfo=UTC)

        stage = StopAt[int](stop_at=stop_at)

        assert stage.stop_at == stop_at
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize("tz", [UTC, None], ids=["aware", "naive"])
    async def test_frames_pass_through_before_stop_at(self, tz: tzinfo | None) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> StopAt[int](stop_at=datetime.now(tz) + timedelta(hours=1))

        result = await _collect(stage)

        assert result == [1, 2, 3]
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize("tz", [UTC, None], ids=["aware", "naive"])
    async def test_stop_at_in_the_past_raises_timeout_error_without_pulling(
        self, tz: tzinfo | None
    ) -> None:
        pulled: list[int] = []

        def frames() -> Iterator[int]:
            for frame in (1, 2, 3):
                pulled.append(frame)
                yield frame

        stage = IterableSource(source=frames()) >> StopAt[int](
            stop_at=datetime.now(tz) - timedelta(seconds=1)
        )
        await stage.mount()

        with pytest.raises(TimeoutError):
            await stage.__anext__()

        assert pulled == []
        assert stage.state is ElementState.ERROR

    async def test_raises_timeout_error_once_stop_at_is_reached_between_frames(
        self,
    ) -> None:
        stop_at = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
        before = stop_at - timedelta(seconds=1)
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> StopAt[int](stop_at=stop_at)
        await stage.mount()

        with patch("pyrill.time.datetime") as mock_datetime:
            mock_datetime.now.side_effect = _clock(before, before, then=stop_at)
            frames = [await stage.__anext__(), await stage.__anext__()]
            with pytest.raises(TimeoutError):
                await stage.__anext__()

        assert frames == [1, 2]
        assert stage.state is ElementState.ERROR

    async def test_now_is_read_in_the_stop_at_timezone(self) -> None:
        stop_at = datetime(2030, 1, 1, tzinfo=UTC)
        source: IterableSource[int] = IterableSource(source=[1])
        stage = source >> StopAt[int](stop_at=stop_at)

        with patch("pyrill.time.datetime") as mock_datetime:
            mock_datetime.now.return_value = stop_at - timedelta(hours=1)
            result = await _collect(stage)

        assert result == [1]
        mock_datetime.now.assert_called_with(UTC)

    async def test_source_blocked_past_stop_at_raises_timeout_error(self) -> None:
        source: QueueSource[int] = QueueSource()
        stop_at = datetime.now(UTC) + timedelta(seconds=0.1)
        stage = source >> StopAt[int](stop_at=stop_at)
        await stage.mount()

        outcome = await _settle(stage.__anext__(), within=1.0)

        assert outcome.done()
        assert not outcome.cancelled()
        assert isinstance(outcome.exception(), TimeoutError)
        assert stage.state is ElementState.ERROR

    async def test_frame_arriving_while_waiting_before_stop_at_is_returned(
        self,
    ) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> StopAt[int](stop_at=datetime.now(UTC) + timedelta(hours=1))
        await stage.mount()
        task = asyncio.ensure_future(stage.__anext__())
        await asyncio.sleep(0.01)  # let the stage start waiting on its source
        await source.push_frame(5)

        result = await asyncio.wait_for(task, timeout=1)

        assert result == 5
        assert stage.state is ElementState.READY

    async def test_timeout_error_from_the_source_is_propagated(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> StopAt[int](stop_at=datetime.now(UTC) + timedelta(hours=1))
        await stage.mount()
        await source.push_frame(TimeoutError("upstream"))

        outcome = await _settle(stage.__anext__(), within=1.0)

        assert outcome.done()
        assert not outcome.cancelled()
        assert isinstance(outcome.exception(), TimeoutError)
        assert stage.state is ElementState.ERROR


    async def test_upstream_timeout_stage_error_is_propagated(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = (
            source
            >> Timeout[int](timeout=0.05)
            >> StopAt[int](stop_at=datetime.now(UTC) + timedelta(hours=1))
        )
        await stage.mount()

        outcome = await _settle(stage.__anext__(), within=1.0)

        assert outcome.done()
        assert not outcome.cancelled()
        assert isinstance(outcome.exception(), TimeoutError)
        assert stage.state is ElementState.ERROR

    async def test_cancellation_before_stop_at_is_not_swallowed(self) -> None:
        source: QueueSource[int] = QueueSource()
        stage = source >> StopAt[int](stop_at=datetime.now(UTC) + timedelta(hours=1))
        await stage.mount()
        task = asyncio.ensure_future(stage.__anext__())
        await asyncio.sleep(0.01)  # let the stage start waiting on its source

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        assert task.cancelled()


# ---------------------------------------------------------------------------
# TestStartAt
# ---------------------------------------------------------------------------


class TestStartAt:
    def test_init_sets_start_at(self) -> None:
        start_at = datetime(2030, 1, 1, tzinfo=UTC)

        stage = StartAt[int](start_at=start_at)

        assert stage.start_at == start_at
        assert stage.state is ElementState.NULL

    async def test_sleeps_until_start_at_before_the_first_frame(self) -> None:
        start_at = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
        source: IterableSource[int] = IterableSource(source=[1])
        stage = source >> StartAt[int](start_at=start_at)
        await stage.mount()

        with (
            patch("pyrill.time.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("pyrill.time.datetime") as mock_datetime,
        ):
            mock_datetime.now.return_value = start_at - timedelta(seconds=5)
            result = await stage.__anext__()

        assert result == 1
        mock_sleep.assert_awaited_once_with(5.0)

    async def test_stops_sleeping_once_start_at_has_passed(self) -> None:
        start_at = datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        stage = source >> StartAt[int](start_at=start_at)

        with (
            patch("pyrill.time.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("pyrill.time.datetime") as mock_datetime,
        ):
            mock_datetime.now.side_effect = _clock(
                start_at - timedelta(seconds=5), then=start_at
            )
            result = await _collect(stage)

        assert result == [1, 2, 3]
        mock_sleep.assert_awaited_once_with(5.0)
        assert stage.state is ElementState.NULL

    @pytest.mark.parametrize("tz", [UTC, None], ids=["aware", "naive"])
    async def test_start_at_in_the_past_does_not_sleep(self, tz: tzinfo | None) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2])
        stage = source >> StartAt[int](start_at=datetime.now(tz) - timedelta(hours=1))

        with patch("pyrill.time.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await _collect(stage)

        assert result == [1, 2]
        mock_sleep.assert_not_awaited()

    @pytest.mark.parametrize("tz", [UTC, None], ids=["aware", "naive"])
    async def test_now_is_read_in_the_start_at_timezone(
        self, tz: tzinfo | None
    ) -> None:
        start_at = datetime(2030, 1, 1, tzinfo=tz)
        source: IterableSource[int] = IterableSource(source=[1])
        stage = source >> StartAt[int](start_at=start_at)

        with patch("pyrill.time.datetime") as mock_datetime:
            mock_datetime.now.return_value = start_at + timedelta(hours=1)
            result = await _collect(stage)

        assert result == [1]
        mock_datetime.now.assert_called_with(tz)

    async def test_first_frame_is_not_emitted_before_start_at(self) -> None:
        start_at = datetime.now(UTC) + timedelta(seconds=0.05)
        source: IterableSource[int] = IterableSource(source=[1])
        stage = source >> StartAt[int](start_at=start_at)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 1
        assert datetime.now(UTC) >= start_at - timedelta(milliseconds=5)

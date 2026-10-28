from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pyrill.base import ElementState
from pyrill.sources import AsyncIterableSource, BuilderSource, IterableSource, builder


async def _async_gen(*values: Any) -> AsyncIterator[Any]:
    for v in values:
        yield v


class TestIterableSource:
    async def test_source_property_returns_stored_iterable(self) -> None:
        data: list[int] = [1, 2, 3]
        source: IterableSource[int] = IterableSource(source=data)

        result = source.source

        assert result is data

    async def test_mount_transitions_state_to_ready(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])

        await source.mount()

        assert source.state is ElementState.READY

    async def test_unmount_transitions_state_to_null(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        await source.mount()

        await source.unmount()

        assert source.state is ElementState.NULL

    async def test_anext_returns_first_element_and_stays_ready(self) -> None:
        source: IterableSource[int] = IterableSource(source=[10, 20, 30])

        await source.mount()
        result = await source.__anext__()

        assert result == 10
        assert source.state is ElementState.READY

    async def test_anext_on_exhausted_source_raises_and_unmounts(self) -> None:
        source: IterableSource[int] = IterableSource(source=[42])
        await source.mount()
        await source.__anext__()

        with pytest.raises(StopAsyncIteration):
            await source.__anext__()

        assert source.state is ElementState.NULL

    @pytest.mark.parametrize(
        "data",
        [
            [1, 2, 3],
            ["a", "b"],
            [True, False, True],
        ],
    )
    async def test_full_iteration_yields_all_elements(self, data: list[Any]) -> None:
        source: IterableSource[Any] = IterableSource(source=data)

        await source.mount()
        result = [item async for item in source]

        assert result == data
        assert source.state is ElementState.NULL

    async def test_full_iteration_of_empty_iterable_yields_nothing(self) -> None:
        source: IterableSource[int] = IterableSource(source=[])

        await source.mount()
        result = [item async for item in source]

        assert result == []
        assert source.state is ElementState.NULL


class TestAsyncIterableSource:
    async def test_source_property_returns_stored_async_iterable(self) -> None:
        data: AsyncIterator[int] = _async_gen(1, 2, 3)
        source: AsyncIterableSource[int] = AsyncIterableSource(source=data)

        result = source.source

        assert result is data

    async def test_mount_transitions_state_to_ready(self) -> None:
        source: AsyncIterableSource[int] = AsyncIterableSource(source=_async_gen(1, 2))

        await source.mount()

        assert source.state is ElementState.READY

    async def test_unmount_transitions_state_to_null(self) -> None:
        source: AsyncIterableSource[int] = AsyncIterableSource(source=_async_gen(1, 2))
        await source.mount()

        await source.unmount()

        assert source.state is ElementState.NULL

    async def test_anext_returns_first_element_and_stays_ready(self) -> None:
        source: AsyncIterableSource[int] = AsyncIterableSource(
            source=_async_gen(10, 20, 30)
        )

        await source.mount()
        result = await source.__anext__()

        assert result == 10
        assert source.state is ElementState.READY

    async def test_anext_on_exhausted_source_raises_and_unmounts(self) -> None:
        source: AsyncIterableSource[int] = AsyncIterableSource(source=_async_gen(42))
        await source.mount()
        await source.__anext__()

        with pytest.raises(StopAsyncIteration):
            await source.__anext__()

        assert source.state is ElementState.NULL

    @pytest.mark.parametrize(
        "data",
        [
            [1, 2, 3],
            ["a", "b"],
            [True, False, True],
        ],
    )
    async def test_full_iteration_yields_all_elements(self, data: list[Any]) -> None:
        source: AsyncIterableSource[Any] = AsyncIterableSource(source=_async_gen(*data))

        await source.mount()
        result = [item async for item in source]

        assert result == data
        assert source.state is ElementState.NULL

    async def test_full_iteration_of_empty_async_iterable_yields_nothing(self) -> None:
        source: AsyncIterableSource[int] = AsyncIterableSource(source=_async_gen())

        await source.mount()
        result = [item async for item in source]

        assert result == []
        assert source.state is ElementState.NULL


class TestBuilderSource:
    async def test_anext_calls_builder_and_returns_result(self) -> None:
        mock_builder: AsyncMock = AsyncMock(return_value=99)
        source: BuilderSource[int] = BuilderSource(builder=mock_builder)

        await source.mount()
        result = await source.__anext__()

        assert result == 99
        mock_builder.assert_called_once()
        assert source.state is ElementState.READY

    async def test_anext_calls_builder_on_each_invocation(self) -> None:
        call_count: int = 0

        async def counting_builder() -> int:
            nonlocal call_count
            call_count += 1
            return call_count

        source: BuilderSource[int] = BuilderSource(builder=counting_builder)

        await source.mount()
        first = await source.__anext__()
        second = await source.__anext__()

        assert first == 1
        assert second == 2
        assert call_count == 2
        assert source.state is ElementState.READY

    async def test_anext_on_stop_async_iteration_unmounts(self) -> None:
        async def finite_builder() -> int:
            raise StopAsyncIteration

        source: BuilderSource[int] = BuilderSource(builder=finite_builder)

        await source.mount()
        with pytest.raises(StopAsyncIteration):
            await source.__anext__()

        assert source.state is ElementState.NULL


class TestBuilderDecorator:
    async def test_returns_builder_source_instance(self) -> None:
        mock_fn: AsyncMock = AsyncMock(return_value=1)

        result = builder(mock_fn)

        assert isinstance(result, BuilderSource)
        assert result.state is ElementState.NULL

    async def test_wrapped_source_delegates_to_original_function(self) -> None:
        mock_fn: AsyncMock = AsyncMock(return_value=42)
        source = builder(mock_fn)

        await source.mount()
        value = await source.__anext__()

        assert value == 42
        mock_fn.assert_called_once()
        assert source.state is ElementState.READY

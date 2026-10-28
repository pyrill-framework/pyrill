from collections.abc import Awaitable, Callable

from pyrill.mappers import BaseMap, Map, make_map
from pyrill.sinks import Last
from pyrill.sources import IterableSource

# ---------------------------------------------------------------------------
# TestMap
# ---------------------------------------------------------------------------


class TestMap:
    async def test_sync_func_transforms_single_frame(self) -> None:
        source: IterableSource[int] = IterableSource(source=[5])
        stage: Map[int, int] = source >> Map[int, int](map_func=lambda x: x * 2)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 10

    async def test_async_func_transforms_single_frame(self) -> None:
        async def double(x: int) -> int:
            return x * 2

        source: IterableSource[int] = IterableSource(source=[7])
        stage: Map[int, int] = source >> Map[int, int](map_func=double)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 14

    async def test_all_frames_transformed_in_sequence(self) -> None:
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: Last[int] = (
            source >> Map[int, int](map_func=lambda x: x * 10) >> Last[int]()
        )

        result = await sink.get_frame()

        assert result == 30

    async def test_map_func_stored_as_attribute(self) -> None:
        func: Callable[[int], Awaitable[int] | int] = lambda x: x + 1
        stage: Map[int, int] = Map[int, int](map_func=func)

        assert stage.map_func is func


# ---------------------------------------------------------------------------
# TestMakeMap
# ---------------------------------------------------------------------------


class TestMakeMap:
    async def test_returned_class_adopts_func_metadata(self) -> None:
        """A docstring for metadata test."""

        def transform(x: int) -> int:
            """Doubles x."""
            return x * 2

        Mapper = make_map(transform)

        assert isinstance(Mapper, type)
        assert issubclass(Mapper, BaseMap)
        assert Mapper.__name__ == "transform"
        assert Mapper.__qualname__ == transform.__qualname__
        assert Mapper.__doc__ == "Doubles x."
        assert Mapper.__module__ == transform.__module__

    async def test_sync_func_transforms_single_frame(self) -> None:
        def negate(x: int) -> int:
            return -x

        Mapper = make_map(negate)
        source: IterableSource[int] = IterableSource(source=[3])
        stage: BaseMap[int, int] = source >> Mapper()
        await stage.mount()

        result = await stage.__anext__()

        assert result == -3

    async def test_async_func_transforms_single_frame(self) -> None:
        async def increment(x: int) -> int:
            return x + 1

        Mapper = make_map(increment)
        source: IterableSource[int] = IterableSource(source=[9])
        stage: BaseMap[int, int] = source >> Mapper()
        await stage.mount()

        result = await stage.__anext__()

        assert result == 10

    async def test_all_frames_transformed_in_sequence(self) -> None:
        def square(x: int) -> int:
            return x * x

        Mapper = make_map(square)
        source: IterableSource[int] = IterableSource(source=[1, 2, 3])
        sink: Last[int] = source >> Mapper() >> Last[int]()

        result = await sink.get_frame()

        assert result == 9

    async def test_keyword_only_param_extracted_and_forwarded(self) -> None:
        def scale(x: int, *, factor: int) -> int:
            return x * factor

        Mapper = make_map(scale)
        source: IterableSource[int] = IterableSource(source=[4])
        stage: BaseMap[int, int] = source >> Mapper(factor=3)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 12

    async def test_multiple_keyword_only_params_forwarded(self) -> None:
        def affine(x: int, *, scale: int, offset: int) -> int:
            return x * scale + offset

        Mapper = make_map(affine)
        source: IterableSource[int] = IterableSource(source=[5])
        stage: BaseMap[int, int] = source >> Mapper(scale=2, offset=1)
        await stage.mount()

        result = await stage.__anext__()

        assert result == 11

    async def test_keyword_only_param_with_default_needs_no_kwarg(self) -> None:
        def add(x: int, *, delta: int = 100) -> int:
            return x + delta

        Mapper = make_map(add)
        source: IterableSource[int] = IterableSource(source=[1])
        stage: BaseMap[int, int] = source >> Mapper()
        await stage.mount()

        result = await stage.__anext__()

        assert result == 101

    async def test_non_keyword_only_params_not_consumed_from_kwargs(self) -> None:
        call_args: list[tuple[int, int]] = []

        def record(x: int, *, multiplier: int) -> int:
            call_args.append((x, multiplier))
            return x * multiplier

        Mapper = make_map(record)
        source: IterableSource[int] = IterableSource(source=[6])
        stage: BaseMap[int, int] = source >> Mapper(multiplier=7)
        await stage.mount()

        await stage.__anext__()

        assert call_args == [(6, 7)]

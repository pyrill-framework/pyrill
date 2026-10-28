===============
Custom elements
===============

The elements that PyRill provides cover common cases. For everything else you write
your own. This guide goes from the quickest option to the most flexible one, and
ends with packaging a chain of elements as one.

Quick stages from functions
---------------------------

For a one-off transformation, pass a function to :class:`~pyrill.mappers.Map`. The
function can also be a coroutine function, and PyRill awaits its result. See
:doc:`../quickstart` for examples.

When you want a reusable stage with options, decorate a function with
:func:`~pyrill.mappers.make_map`. The decorator turns it into a stage *class*. The
first parameter is the frame and must be positional-only (the ``/``). Its
keyword-only parameters become the keyword arguments of the class:

.. code-block:: python

    import asyncio
    from typing import Any

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import make_map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    @make_map
    def Scale(frame: float, /, *, factor: float = 1.0, **kwargs: Any) -> float:
        return frame * factor


    async def main() -> None:
        sink = IterableSource(source=[1.0, 2.0]) >> Scale(factor=10) >> ListAcc() >> Last()
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [10.0, 20.0]

Some of PyRill's own stages, such as :class:`~pyrill.bytes.Decode` and
:class:`~pyrill.strings.Upper`, are made in the same way.

A stage with its own logic
--------------------------

Subclass :class:`~pyrill.base.BaseStage` when the stage needs state or more control.
Implement ``_process_frame``, which receives one frame and returns one frame. Raise
:class:`~pyrill.base.FrameSkipped` to drop the frame instead. Override ``_mount``
to set up or reset state, and always call ``super()``:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseStage, FrameSkipped
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    class DropRepeats(BaseStage[str, str]):
        """Pass on a frame only when it differs from the previous one."""

        _last: str | None = None

        async def _mount(self) -> None:
            self._last = None
            await super()._mount()

        async def _process_frame(self, frame: str) -> str:
            if frame == self._last:
                raise FrameSkipped(frame)
            self._last = frame
            return frame


    async def main() -> None:
        sink = (
            IterableSource(source=["a", "a", "b", "b", "a"])
            >> DropRepeats()
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    ['a', 'b', 'a']

The two type arguments of ``BaseStage[str, str]`` are the frame type it reads and the
frame type it produces. Resetting ``_last`` in ``_mount`` makes the stage behave the
same every time the pipeline runs.

One frame in, many frames out
-----------------------------

A stage that returns exactly one frame per input is not the right tool for splitting.
:class:`~pyrill.primitives.FlatMap` maps each frame to a *source* and emits
everything that source produces, one frame after another. Here each number ``n``
becomes ``n`` copies of itself:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.primitives import FlatMap
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        sink = (
            IterableSource(source=[1, 2, 3])
            >> FlatMap(map_func=lambda n: IterableSource(source=[n] * n))
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [1, 2, 2, 3, 3, 3]

Use :class:`~pyrill.lists.FlatIterable` instead when each frame is already a
collection and you want its items.

A custom source
---------------

Subclass :class:`~pyrill.base.BaseSource` and implement ``_next_frame``. Return the
next frame, or raise :class:`StopAsyncIteration` when there are no more. The method
is called once per frame, and only when somebody asks for one:

.. code-block:: python

    import asyncio
    from typing import Any

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseSource
    from pyrill.sinks import Last


    class Countdown(BaseSource[int]):
        """Produce start, start - 1, ..., 1."""

        def __init__(self, *args: Any, start: int, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._start = start
            self._next = start

        async def _mount(self) -> None:
            self._next = self._start
            await super()._mount()

        async def _next_frame(self) -> int:
            if self._next < 1:
                raise StopAsyncIteration()
            value = self._next
            self._next -= 1
            return value


    async def main() -> None:
        sink = Countdown(start=3) >> ListAcc() >> Last()
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [3, 2, 1]

Pass ``*args`` and ``**kwargs`` on to ``super().__init__()``, so that the standard
options such as ``name`` and ``pipeline`` keep working.

A custom sink
-------------

Subclass :class:`~pyrill.base.BaseFullSink` for a sink that does something with
every frame. ``_process_frame`` is called for each one, and ``consume_all()`` runs
the pipeline until the end:

.. code-block:: python

    import asyncio
    from typing import Any

    from pyrill.base import BaseFullSink, BaseSource


    class Countdown(BaseSource[int]):
        def __init__(self, *args: Any, start: int, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._start = start
            self._next = start

        async def _mount(self) -> None:
            self._next = self._start
            await super()._mount()

        async def _next_frame(self) -> int:
            if self._next < 1:
                raise StopAsyncIteration()
            value = self._next
            self._next -= 1
            return value


    class Collect(BaseFullSink[int]):
        """Keep every frame in a list."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.items: list[int] = []

        async def _process_frame(self, frame: int) -> None:
            self.items.append(frame)


    async def main() -> None:
        sink = Countdown(start=3) >> Collect()
        await sink.consume_all()
        print(sink.items)


    asyncio.run(main())

.. code-block:: text

    [3, 2, 1]

Package a chain as one stage
----------------------------

A chain of stages that you use together can become a single element. Write a *builder
function* that creates the stages, connects them, and returns the first and the last
one. The :func:`~pyrill.primitives.bin_stage` decorator turns it into a stage class:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseConsumer, BaseProducer
    from pyrill.bytes import Decode
    from pyrill.primitives import SkipIf, bin_stage
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.strlike import Strip


    @bin_stage
    def clean_lines() -> tuple[BaseConsumer[bytes], BaseProducer[str]]:
        """Decode lines, remove the spaces around them and drop empty ones."""
        decode = Decode()
        cleaned = decode >> Strip() >> SkipIf(check_func=bool)
        return decode, cleaned


    async def main() -> None:
        sink = (
            IterableSource(source=[b"  a \n", b"\n", b"b\n"])
            >> clean_lines()
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    ['a', 'b']

Every use of ``clean_lines()`` gets its own fresh copy of the inner stages, so you can
use it in as many pipelines as you like. Keyword-only parameters of the builder become
keyword arguments of the class. For a bin whose inner elements are created in a
method, subclass :class:`~pyrill.primitives.BaseBin` and implement
``_get_upstream_downstream``. :class:`~pyrill.primitives.Bin` takes the two ends as
arguments, if you already have them.

Where to look next
------------------

The :doc:`catalog <../catalog/index>` page for :mod:`pyrill.base` documents every base
class with the methods you can override.

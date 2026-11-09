====================
Work with windows
====================

:class:`~pyrill.windows.WindowSize` groups consecutive frames into windows of at
most ``size`` frames. Each window is a :class:`~pyrill.base.BaseProducer`; connect
it to a sink to consume its frames. The concrete producer type is an
implementation detail.

Windows preserve input order. The last window may contain fewer than ``size``
frames, and an empty source emits no windows. ``size`` must be positive;
zero or negative values raise :class:`ValueError`.

Collect each window as a list
-----------------------------

Use :class:`~pyrill.mappers.Map` with an asynchronous function to consume each
window. Inside that function, connect the producer to
:class:`~pyrill.accumulators.ListAcc` and :class:`~pyrill.sinks.Last` to obtain
one list. A second accumulator and sink collect those lists into the final
result:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseProducer
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.windows import WindowSize


    async def collect_window(window: BaseProducer[int]) -> list[int]:
        sink = window >> ListAcc[int]() >> Last[list[int]]()
        return await sink.get_frame()


    async def main() -> None:
        sink = (
            IterableSource(source=[1, 2, 3, 4, 5])
            >> WindowSize[int](size=2)
            >> Map[BaseProducer[int], list[int]](map_func=collect_window)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [[1, 2], [3, 4], [5]]

The inner sink consumes one window; the outer sink drives the whole pipeline.
``get_frame()`` handles mounting and unmounting, so these examples need no
manual lifecycle calls. ``Last`` raises :class:`ValueError` when its input is
empty; in this example that happens if the original source emits no frames.

Calculate a total for each window
---------------------------------

Connect each window to :class:`~pyrill.accumulators.SumAcc` and ``Last`` to
compute a total without creating a list of its frames. Each invocation creates
a fresh accumulator, so totals do not carry over between windows:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc, SumAcc
    from pyrill.base import BaseProducer
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.windows import WindowSize


    async def total_window(window: BaseProducer[int]) -> int:
        sink = window >> SumAcc[int]() >> Last[int]()
        return await sink.get_frame()


    async def main() -> None:
        sink = (
            IterableSource(source=[10, 20, 30, 40, 50])
            >> WindowSize[int](size=2)
            >> Map[BaseProducer[int], int](map_func=total_window)
            >> ListAcc[int]()
            >> Last[list[int]]()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [30, 70, 50]

Process batches without collecting the whole stream
---------------------------------------------------

When a pipeline performs an action for each batch, finish it with
:class:`~pyrill.sinks.BlackHole`. Its ``consume_all()`` method drives the stream
and discards output frames, including when the input is empty. The mapper below
prints each batch; replace that operation with an asynchronous batch write or
request as needed:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseProducer
    from pyrill.mappers import Map
    from pyrill.sinks import BlackHole, Last
    from pyrill.sources import IterableSource
    from pyrill.windows import WindowSize


    async def process_window(window: BaseProducer[str]) -> None:
        sink = window >> ListAcc[str]() >> Last[list[str]]()
        batch = await sink.get_frame()
        print(batch)


    async def main() -> None:
        sink = (
            IterableSource(source=["a", "b", "c", "d", "e"])
            >> WindowSize[str](size=2)
            >> Map[BaseProducer[str], None](map_func=process_window)
            >> BlackHole[None]()
        )
        await sink.consume_all()


    asyncio.run(main())

.. code-block:: text

    ['a', 'b']
    ['c', 'd']
    ['e']

``Map`` awaits each batch operation before requesting the next window. This
pattern processes windows sequentially and retains no list of past batches.
Collecting all window producers for later consumption instead keeps their
buffered frames in memory. ``size`` controls frames per window, rather than a
time interval or a limit on concurrent operations.

See :doc:`../catalog/windows` for the API reference and
:doc:`concurrent-tasks` for scheduling asynchronous operations concurrently.

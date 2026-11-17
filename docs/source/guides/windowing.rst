============================
Work with windows and groups
============================

:class:`~pyrill.windows.WindowSize` partitions frames by count,
:class:`~pyrill.windows.WindowTime` closes windows after a timeout, and
:class:`~pyrill.windows.GroupBy` routes frames with equal keys to the same group.
Each output is a :class:`~pyrill.base.BaseProducer`; connect it to a sink to
consume its frames. The concrete producer type is an implementation detail.

With ``WindowSize``, the last window may contain fewer than ``size`` frames,
and an empty source emits no windows. ``size`` must be positive; zero or negative
values raise :class:`ValueError`.

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

Close windows after a timeout
-----------------------------

``WindowTime`` takes a positive :class:`~datetime.timedelta`. Its timer starts
when a nonempty window is returned downstream. Frames arriving before that
timer expires belong to the same window. Expiry closes the window, and the next
frame starts another one. These deadlines are relative to each emitted window;
they are not aligned to fixed wall-clock boundaries.

This source emits two frames together, pauses, then emits a third frame. Both
the outer stream and each window are consumed by sinks:

.. code-block:: python

    import asyncio
    from collections.abc import AsyncIterator
    from datetime import timedelta

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseProducer
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import AsyncIterableSource
    from pyrill.windows import WindowTime


    async def frames() -> AsyncIterator[int]:
        yield 1
        yield 2
        await asyncio.sleep(0.05)
        yield 3
        await asyncio.sleep(0.05)


    async def collect_window(window: BaseProducer[int]) -> list[int]:
        sink = window >> ListAcc[int]() >> Last[list[int]]()
        return await sink.get_frame()


    async def main() -> None:
        sink = (
            AsyncIterableSource(source=frames())
            >> WindowTime[int](timeout=timedelta(milliseconds=10))
            >> Map[BaseProducer[int], list[int]](map_func=collect_window)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )
        print(await sink.get_frame())


    asyncio.run(main())

Typical output:

.. code-block:: text

    [[1, 2], [3]]

Boundaries depend on frame arrival and event-loop scheduling. Idle time produces
no empty windows, and upstream exhaustion closes the final nonempty window
immediately. There is no frame-count limit, so a busy source may put many frames
into a single timed window.

Group frames by a key
---------------------

``GroupBy`` calls its synchronous ``key_builder`` once for each frame. It emits
one producer for each new hashable key, in first-seen key order. Frames within a
group retain input order even when other keys occur between them:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseProducer
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.windows import GroupBy


    async def collect_group(group: BaseProducer[int]) -> list[int]:
        sink = group >> ListAcc[int]() >> Last[list[int]]()
        return await sink.get_frame()


    async def main() -> None:
        sink = (
            IterableSource(source=[1, 2, 3, 4, 5])
            >> GroupBy[int](key_builder=lambda frame: frame % 2)
            >> Map[BaseProducer[int], list[int]](map_func=collect_group)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [[1, 3, 5], [2, 4]]

This groups all matching frames, rather than splitting the source whenever its
key changes. Group producers supply original frames; their output is not a
``(key, producer)`` tuple. Groups close when upstream ends or the stage is
unmounted, rather than after a fixed number of frames or a timeout.

The example uses a finite source so each inner sink can finish. For a continuing
source, a sink that waits for a group's end also waits for upstream exhaustion.
Group buffers are unbounded, so groups that are not being consumed retain their
frames in memory.

Read the key associated with a group
------------------------------------

:class:`~pyrill.windows.GroupQueue` is a queue source with a read-only
:attr:`~pyrill.windows.GroupQueue.key` property. The key is the original hashable
object supplied when the group was created. Reading or remounting the source
does not change it; ``None``, ``0`` and empty strings are valid keys.

``GroupBy`` exposes its outputs as producers. Check that a producer is a
``GroupQueue`` before accessing the additional ``key`` property, then connect
it to a sink as usual. This example returns both the key and frames of each
group:

.. code-block:: python

    import asyncio
    from collections.abc import Hashable

    from pyrill.accumulators import ListAcc
    from pyrill.base import BaseProducer
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.windows import GroupBy, GroupQueue


    async def collect_group(
        group: BaseProducer[int],
    ) -> tuple[Hashable, list[int]]:
        assert isinstance(group, GroupQueue)
        sink = group >> ListAcc[int]() >> Last[list[int]]()
        frames = await sink.get_frame()
        return group.key, frames


    async def main() -> None:
        sink = (
            IterableSource(source=[1, 2, 3, 4, 5])
            >> GroupBy[int](key_builder=lambda frame: frame % 2)
            >> Map[BaseProducer[int], tuple[Hashable, list[int]]](map_func=collect_group)
            >> ListAcc[tuple[Hashable, list[int]]]()
            >> Last[list[tuple[Hashable, list[int]]]]()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [(1, [1, 3, 5]), (0, [2, 4])]

``GroupQueue`` also supports the usual :class:`~pyrill.queues.QueueSource`
methods when used directly: mount it, push frames, and close it with
``end_of_stream()``. The :doc:`../catalog/windows` example shows this usage.

Observe frames rejected by the key builder
------------------------------------------

If ``key_builder`` raises an exception, ``GroupBy`` skips that frame, logs a
warning, and emits a :data:`~pyrill.base.MSG_FRAME_SKIPPED` message. Subscribe on
the stage to inspect the rejected frame while valid frames continue:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.base import MSG_FRAME_SKIPPED, BaseProducer, Message, MessageBrokerMixin
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.windows import GroupBy


    def build_key(frame: int) -> int:
        if frame == 0:
            raise ValueError("zero has no group")
        return frame % 2


    async def collect_group(group: BaseProducer[int]) -> list[int]:
        sink = group >> ListAcc[int]() >> Last[list[int]]()
        return await sink.get_frame()


    async def main() -> None:
        skipped: list[Message] = []

        def on_skip(broker: MessageBrokerMixin, message: Message) -> None:
            skipped.append(message)

        stage = IterableSource(source=[1, 0, 2, 3, 4]) >> GroupBy[int](
            key_builder=build_key
        )
        stage.add_message_callback(on_skip, MSG_FRAME_SKIPPED)
        sink = (
            stage
            >> Map[BaseProducer[int], list[int]](map_func=collect_group)
            >> ListAcc[list[int]]()
            >> Last[list[list[int]]]()
        )
        print(await sink.get_frame())
        print([message.params["frame"] for message in skipped])


    asyncio.run(main())

.. code-block:: text

    [[1, 3], [2, 4]]
    [0]

:class:`~pyrill.windows.BaseWindow` supplies the shared lifecycle for custom
window boundaries. See :doc:`../catalog/windows` for its hooks and API reference,
:doc:`messages-and-monitoring` for message subscriptions, and
:doc:`concurrent-tasks` for scheduling asynchronous operations concurrently.

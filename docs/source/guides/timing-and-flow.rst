===============
Timing and flow
===============

Some pipelines have to care about time: they must not go too fast, they must give
up when something is too slow, or they must run only during a window. Others need
to be paused, or fed from code that is not an iterator. This guide covers the
elements for all of that.

Slow a stream down
------------------

:class:`~pyrill.time.Delayed` waits ``delay`` seconds before it passes on each
frame. Use it to throttle a pipeline, for example to stay under the rate limit of
an API:

.. code-block:: python

    import asyncio
    import time

    from pyrill.accumulators import ListAcc
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.time import Delayed


    async def main() -> None:
        started = time.perf_counter()
        sink = (
            IterableSource(source=[1, 2, 3])
            >> Delayed(delay=0.02)
            >> ListAcc()
            >> Last()
        )

        print(await sink.get_frame())
        print(time.perf_counter() - started >= 0.05)


    asyncio.run(main())

.. code-block:: text

    [1, 2, 3]
    True

Give each frame a deadline
--------------------------

:class:`~pyrill.time.Timeout` limits how long the stage waits for the next frame.
If the frame does not arrive in time, the stage raises :class:`TimeoutError`. The
limit applies to every frame separately, not to the whole stream, so a long stream
of quick frames is fine:

.. code-block:: python

    import asyncio

    from pyrill.queues import QueueSource
    from pyrill.sinks import Last
    from pyrill.time import Timeout


    async def main() -> None:
        silent = QueueSource[int]()  # nobody pushes frames into it
        sink = silent >> Timeout(timeout=0.05) >> Last()

        try:
            await sink.get_frame()
        except TimeoutError:
            print("no frame within 0.05 seconds")


    asyncio.run(main())

.. code-block:: text

    no frame within 0.05 seconds

Run only inside a time window
-----------------------------

:class:`~pyrill.time.StartAt` holds the first frame until a given moment, and
:class:`~pyrill.time.StopAt` stops the stream at a given moment. Both take a
:class:`~datetime.datetime`, naive or timezone-aware.

This stream waits for 0.05 seconds before it lets the frame through:

.. code-block:: python

    import asyncio
    from datetime import UTC, datetime, timedelta

    from pyrill.sinks import Last
    from pyrill.sources import IterableSource
    from pyrill.time import StartAt


    async def main() -> None:
        start_at = datetime.now(UTC) + timedelta(seconds=0.05)
        sink = IterableSource(source=["go"]) >> StartAt(start_at=start_at) >> Last()
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    go

``StopAt`` raises :class:`TimeoutError` when the moment arrives, whether the stage
is waiting for a frame or the moment had already passed. Catch it where you read
the pipeline. This example reads an endless ticker for 0.05 seconds:

.. code-block:: python

    import asyncio
    from collections.abc import AsyncIterator
    from datetime import UTC, datetime, timedelta

    from pyrill.sources import AsyncIterableSource
    from pyrill.time import StopAt


    async def ticker() -> AsyncIterator[int]:
        count = 0
        while True:
            await asyncio.sleep(0.01)
            count += 1
            yield count


    async def main() -> None:
        stop_at = datetime.now(UTC) + timedelta(seconds=0.05)
        stage = AsyncIterableSource(source=ticker()) >> StopAt(stop_at=stop_at)
        await stage.mount()

        received: list[int] = []
        try:
            async for frame in stage:
                received.append(frame)
        except TimeoutError:
            print("deadline reached")
        print(len(received) > 0)


    asyncio.run(main())

.. code-block:: text

    deadline reached
    True

Pause and resume
----------------

:class:`~pyrill.primitives.Gate` holds frames back while it is closed. It starts
*closed*. Call ``open()`` to let frames through and ``close()`` to hold them again,
from any task. While the gate is closed it does not read from its source, which
therefore waits too:

.. code-block:: python

    import asyncio

    from pyrill.primitives import Gate
    from pyrill.sources import IterableSource


    async def main() -> None:
        gate = IterableSource(source=[1, 2, 3]) >> Gate[int]()

        async def read() -> list[int]:
            await gate.mount()
            return [frame async for frame in gate]

        async def open_later() -> None:
            await asyncio.sleep(0.01)
            gate.open()

        frames, _ = await asyncio.gather(read(), open_later())
        print(frames)


    asyncio.run(main())

.. code-block:: text

    [1, 2, 3]

The reader waits at the closed gate until ``open_later()`` opens it. Use the same
pattern to pause a stream on user request, on a full buffer downstream, or while a
connection is down.

Push frames in from your own code
---------------------------------

Not every data source is an iterator. Callbacks, message handlers and other tasks
produce data when they feel like it.
:class:`~pyrill.queues.QueueSource` is a source you feed yourself: call
``push_frame()`` to add a frame and push ``StopAsyncIteration()`` to end the stream:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.queues import QueueSource
    from pyrill.sinks import Last


    async def main() -> None:
        source = QueueSource[str]()
        sink = source >> Map(map_func=str.upper) >> ListAcc() >> Last()
        await source.mount()  # it must be mounted before frames are pushed

        async def produce() -> None:
            for word in ["a", "b", "c"]:
                await source.push_frame(word)
            await source.push_frame(StopAsyncIteration())

        _, result = await asyncio.gather(produce(), sink.get_frame())
        print(result)


    asyncio.run(main())

.. code-block:: text

    ['A', 'B', 'C']

Pass ``queue_size`` to limit how many frames can wait. When the queue is full,
``push_frame()`` waits, so a fast producer cannot run far ahead of the pipeline.

Let the producer run ahead
--------------------------

Because pipelines are pull-based, a source produces nothing until a frame is
requested. :class:`~pyrill.queues.Queue` changes that for one stage: a background
task keeps reading from the source and fills a queue, while the next element reads
from the queue at its own pace. Use it to smooth out a slow consumer, or to overlap
waiting on I/O with processing.

.. code-block:: python

    import asyncio
    from collections.abc import AsyncIterator

    from pyrill.queues import Queue
    from pyrill.sources import AsyncIterableSource

    produced: list[int] = []


    async def numbers() -> AsyncIterator[int]:
        for n in range(10):
            produced.append(n)
            yield n


    async def main() -> None:
        stage = AsyncIterableSource(source=numbers()) >> Queue[int](queue_size=3)
        await stage.mount()
        await asyncio.sleep(0.05)  # a consumer that is not ready yet

        print(len(produced) > 1)
        print(len(produced) <= 5)


    asyncio.run(main())

.. code-block:: text

    True
    True

Before anything was read, the source already produced several frames, but it stopped
a few frames ahead because the queue holds three. Without the ``Queue`` it would have
produced none.

=====================
Branching and merging
=====================

A pipeline does not have to be a straight line. This guide shows how to send one
stream to several consumers, how to combine several streams into one, and how to
merge live feeds.

.. list-table::
    :header-rows: 1
    :widths: 22 78

    * - Element
      - What it does
    * - :class:`~pyrill.primitives.Tee`
      - Copies every frame to several consumers.
    * - :class:`~pyrill.primitives.Zip`
      - Pairs one frame from each source into a tuple.
    * - :class:`~pyrill.primitives.Union`
      - Merges several sources into a single stream.

Send one stream to several consumers
------------------------------------

:class:`~pyrill.primitives.Tee` gives every connected consumer every frame. Connect
each branch with ``>>``, then read the branches *concurrently*, for example with
:func:`asyncio.gather`:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc, SumAcc
    from pyrill.mappers import Map
    from pyrill.primitives import Tee
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        numbers = IterableSource(source=[1, 2, 3]) >> Tee[int]()
        total = numbers >> SumAcc() >> Last()
        squares = numbers >> Map(map_func=lambda n: n * n) >> ListAcc() >> Last()

        print(await asyncio.gather(total.get_frame(), squares.get_frame()))


    asyncio.run(main())

.. code-block:: text

    [6, [1, 4, 9]]

The source is read once, and both branches see all of its frames. A few rules
apply:

* Read the branches at the same time. A frame is delivered to all the running
  branches together, so the slowest one sets the pace. A branch starts when it is
  mounted, which ``get_frame()`` does for you. A branch that has started and that
  nobody reads stalls the others. A branch that is only connected, and never
  started, takes no part.
* A branch that finishes early, such as one that ends in
  :class:`~pyrill.sinks.First`, leaves the tee and the other branches continue.
* The tee unmounts when its source ends or its last branch is gone.
* Always connect consumers to the tee. The tee itself does not support ``anext()``.

Connect several branches at once
--------------------------------

``tee >> (a, b)`` connects both stages and returns the tuple, which you can feed to
:class:`~pyrill.primitives.Zip`. This computes two values from each frame and puts
them back together:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.primitives import Tee, Zip
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        sink = (
            IterableSource(source=[1, 2, 3])
            >> Tee[int]()
            >> (Map[int, int](map_func=lambda n: n * 10), Map[int, str](map_func=str))
            >> Zip[int, str]()
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [(10, '1'), (20, '2'), (30, '3')]

Pair frames from several streams
--------------------------------

:class:`~pyrill.primitives.Zip` reads one frame from each of its sources and emits
them as a tuple, in the order the sources were connected. Pass a tuple of sources
with ``>>``, or add them one at a time with ``zip << source``, where ``zip`` is the
``Zip`` element. By default the stream ends as soon as any source runs out:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.primitives import Zip
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        names = IterableSource(source=["Ann", "Bob", "Cleo"])
        ages = IterableSource(source=[30, 25])

        sink = (names, ages) >> Zip[str, int]() >> ListAcc() >> Last()
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [('Ann', 30), ('Bob', 25)]

Set ``exhausted_as_none=True`` to keep going until *every* source is empty. An empty
source then contributes ``None``. The :func:`~pyrill.primitives.zip_sources`
function builds a ``Zip`` and gives each position of the tuple the right type,
including the ``None``:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.primitives import zip_sources
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        names = IterableSource(source=["Ann", "Bob", "Cleo"])
        ages = IterableSource(source=[30, 25])

        sink = zip_sources(names, ages, exhausted_as_none=True) >> ListAcc() >> Last()
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [('Ann', 30), ('Bob', 25), ('Cleo', None)]

If any source raises an exception, the exception is raised by the ``Zip``.

Merge live feeds
----------------

:class:`~pyrill.primitives.Union` reads several sources at the same time and emits
frames in the order they arrive. This is the tool for merging feeds that keep
producing, such as two sensors or two subscriptions:

.. code-block:: python

    import asyncio
    from collections.abc import AsyncIterator

    from pyrill.primitives import Union
    from pyrill.sources import AsyncIterableSource


    async def ticker(name: str, delay: float) -> AsyncIterator[str]:
        while True:
            await asyncio.sleep(delay)
            yield name


    async def main() -> None:
        feeds = Union[str]()
        feeds.add_source(AsyncIterableSource(source=ticker("fast", 0.01)))
        feeds.add_source(AsyncIterableSource(source=ticker("slow", 0.03)))

        await feeds.mount()
        seen: set[str] = set()
        async for frame in feeds:
            seen.add(frame)
            if len(seen) == 2:
                break
        await feeds.unmount()

        print(sorted(seen))


    asyncio.run(main())

.. code-block:: text

    ['fast', 'slow']

Keep two limits in mind:

* The order between different sources is not deterministic, because the sources
  race each other.
* The stream ends as soon as *any* source is exhausted, and the frames that the
  other sources have already read are dropped. ``Union`` is meant for sources that
  do not end, or for cases where stopping at the first end is what you want. To
  consume every source to its end, use ``Zip`` with ``exhausted_as_none=True``, or
  read each source in its own pipeline.

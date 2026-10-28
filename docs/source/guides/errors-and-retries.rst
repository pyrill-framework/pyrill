==================
Errors and retries
==================

Real data is messy: a field is not a number, a file cannot be read, a service does
not answer. This guide explains what happens when a pipeline fails and the tools you
have to deal with it.

What happens when a stage fails
-------------------------------

When code inside a stage raises an exception, the exception travels down the chain
to whoever is reading the pipeline. You handle it there with an ordinary ``try``
statement:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        source = IterableSource(source=["1", "x", "3"])
        convert = source >> Map(map_func=int)
        sink = convert >> ListAcc() >> Last()

        try:
            await sink.get_frame()
        except ValueError as error:
            print("failed:", error)

        print(
            "source:", source.state.name,
            "| convert:", convert.state.name,
            "| sink:", sink.state.name,
        )

        try:
            await convert.mount()
        except RuntimeError as error:
            print("remount:", error)


    asyncio.run(main())

.. code-block:: text

    failed: invalid literal for int() with base 10: 'x'
    source: NULL | convert: ERROR | sink: ERROR
    remount: Element has an error

Three things to notice:

* The stage that failed and everything after it are in the ``ERROR`` state. The
  elements before it are unmounted and release their resources, such as open files.
* An element in ``ERROR`` cannot be mounted again. To run again, build a new
  pipeline.
* The first frame, ``"1"``, was processed normally. The failure happened at the
  second frame, and the stream stopped there.

A failure stops the stream. The next sections show how to avoid failing in the first
place, and how to carry on when a part of the work fails.

Drop frames you cannot use
--------------------------

If bad frames are expected, filter them out before they reach the stage that would
choke on them. :class:`~pyrill.primitives.SkipIf` drops a frame without ending the
stream. It keeps the frames for which the function returns true:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.primitives import SkipIf
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        sink = (
            IterableSource(source=["10", "x", "30"])
            >> SkipIf(check_func=str.isdigit)
            >> Map(map_func=int)
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [10, 30]

In your own stages, raise :class:`~pyrill.base.FrameSkipped` to drop the current
frame. :doc:`custom-elements` has an example.

Turn failures into data
-----------------------

Sometimes you want to keep a record of the bad input instead of losing it. Catch the
error inside the function and return a value that stands for the failure. The stage
never fails, and later stages decide what to do:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    def to_int(text: str) -> int | None:
        try:
            return int(text)
        except ValueError:
            return None


    async def main() -> None:
        sink = (
            IterableSource(source=["10", "x", "30"])
            >> Map(map_func=to_int)
            >> ListAcc()
            >> Last()
        )
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [10, None, 30]

Keep going when one input fails
-------------------------------

:class:`~pyrill.primitives.Retry` is for pipelines that process several inputs, each
one read by its own source (for example one file or one connection each). It reads
those sources one after another and passes on their frames. When a source fails, it
discards that source, keeps the frames it had already delivered and moves on to the
next one. It does not run the failed source again.

``max_retries`` is how many failures in a row it tolerates. The counter goes back to
zero after every successful frame. When the limit is reached, the error is raised.
With ``max_retries=0`` the first failure is raised straight away:

.. code-block:: python

    import asyncio
    from collections.abc import Iterator

    from pyrill.accumulators import ListAcc
    from pyrill.primitives import Retry
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    def unreadable() -> Iterator[int]:
        yield 10
        raise ValueError("unreadable")


    async def run(max_retries: int) -> None:
        inputs = [
            IterableSource(source=[1, 2]),
            IterableSource(source=unreadable()),
            IterableSource(source=[3]),
        ]
        sink = (
            IterableSource(source=inputs)
            >> Retry[int](max_retries=max_retries)
            >> ListAcc()
            >> Last()
        )
        try:
            print(f"max_retries={max_retries}:", await sink.get_frame())
        except ValueError as error:
            print(f"max_retries={max_retries}: raised", repr(error))


    async def main() -> None:
        await run(max_retries=1)
        await run(max_retries=0)


    asyncio.run(main())

.. code-block:: text

    max_retries=1: [1, 2, 10, 3]
    max_retries=0: raised ValueError('unreadable')

By default every :class:`Exception` counts as a failure. Pass ``retry_exceptions``, a
tuple of exception types, to tolerate only those. Any other exception is raised
immediately.

Deadlines
---------

A source that stops answering is a failure too, but it never raises on its own.
:class:`~pyrill.time.Timeout` turns it into a :class:`TimeoutError` after a limit
per frame, and :class:`~pyrill.time.StopAt` does it at a given moment. See
:doc:`timing-and-flow`.

Guidelines
----------

* Catch exceptions where you read the pipeline, not inside every stage.
* Decide for each stage whether a bad frame is expected. If it is, filter it with
  ``SkipIf`` or convert it to data. If it is not, let the exception propagate.
* Do not reuse a pipeline after an error. Create a new one.
* Add :class:`~pyrill.time.Timeout` wherever a pipeline waits for something outside
  your program, such as the network.

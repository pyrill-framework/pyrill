========
Concepts
========

This page explains the ideas behind PyRill. Each one is short, and each comes with
an example you can run.

Frames
------

A *frame* is one item that flows through a pipeline. It can be any Python object: a
line of text, a chunk of bytes, a dictionary, a number. Each element declares the
type of frame it reads and the type it produces, for example
``Map[int, str]`` reads ``int`` frames and produces ``str`` frames.

Elements
--------

Everything in a pipeline is an *element*. There are three kinds:

.. list-table::
    :header-rows: 1
    :widths: 14 30 56

    * - Kind
      - Role
      - Examples
    * - Source
      - Starts a pipeline. It produces frames and reads from no other element.
      - :class:`~pyrill.sources.IterableSource`,
        :class:`~pyrill.io.ChunkedFileSource`,
        :class:`~pyrill.queues.QueueSource`
    * - Stage
      - Sits in the middle. It reads frames from the element before it and produces
        frames for the element after it.
      - :class:`~pyrill.mappers.Map`, :class:`~pyrill.bytes.Decode`,
        :class:`~pyrill.accumulators.ListAcc`
    * - Sink
      - Ends a pipeline. It consumes frames and produces none.
      - :class:`~pyrill.sinks.Last`, :class:`~pyrill.sinks.BlackHole`,
        :class:`~pyrill.io.FileSink`

Some elements are not strictly one of these. :class:`~pyrill.primitives.Tee`
sends its frames to several consumers, and :class:`~pyrill.primitives.Zip` reads
from several sources. The :doc:`catalog <catalog/index>` lists them all.

Connecting elements
-------------------

``a >> b`` connects the output of ``a`` to the input of ``b``, and ``b << a`` does
the same thing. Both return the element on the *downstream* side (``b``), which is
why chains read naturally from left to right:

.. code-block:: python

    import asyncio

    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        source = IterableSource(source=[1, 2, 3])
        double = Map(map_func=lambda n: n * 2)
        sink = Last()

        print((source >> double) is double)
        sink << double
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    True
    6

Elements that take several inputs or outputs accept tuples:
``(source_1, source_2) >> Zip()`` and ``tee >> (stage_1, stage_2)``. See
:doc:`guides/branching-and-merging`.

Frames are pulled
-----------------

PyRill is *pull-based*: a sink asks the element before it for a frame, which asks
the one before it, and so on up to the source. No element produces a frame until
the next one asks for it.

This has a practical effect: a pipeline does only the work that is needed. In this
example the source could produce almost a thousand numbers, but the sink only asks
for the first one:

.. code-block:: python

    import asyncio
    from collections.abc import Iterator

    from pyrill.sinks import First
    from pyrill.sources import IterableSource

    produced: list[int] = []


    def numbers() -> Iterator[int]:
        for n in range(1, 1000):
            produced.append(n)
            yield n


    async def main() -> None:
        sink = IterableSource(source=numbers()) >> First()
        print(await sink.get_frame())
        print(produced)


    asyncio.run(main())

.. code-block:: text

    1
    [1]

It also means that a slow consumer slows down everything upstream. Nothing is
buffered behind its back, unless you add a buffer on purpose with
:class:`~pyrill.queues.Queue`.

Lifecycle
---------

An element moves through states, available as ``element.state``:

``NULL``
    Not mounted. This is the initial state and the state after the stream ends.

``MOUNTING`` and ``READY``
    ``await element.mount()`` prepares the element and everything before it in the
    chain (opens files, creates buffers, starts tasks) and leaves it ``READY`` to
    produce frames.

``UNMOUNTING``
    The element is releasing its resources.

``ERROR``
    Something failed. See :doc:`guides/errors-and-retries`.

You rarely call ``mount()`` and ``unmount()`` yourself. A sink's ``get_frame()`` and
``consume_all()`` mount the pipeline first and unmount it at the end. When you
iterate a stage with ``async for``, you mount it once before the loop, and it
unmounts itself when the stream ends:

.. code-block:: python

    import asyncio

    from pyrill.mappers import Map
    from pyrill.sources import IterableSource


    async def main() -> None:
        stage = IterableSource(source=["a", "b"]) >> Map(map_func=str.upper)
        print(stage.state.name)

        await stage.mount()
        print(stage.state.name)

        async for frame in stage:
            print(frame)
        print(stage.state.name)


    asyncio.run(main())

.. code-block:: text

    NULL
    READY
    A
    B
    NULL

Because the state returns to ``NULL``, a pipeline that has ended can run again. A
source starts from the beginning each time it is mounted:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        sink = IterableSource(source=[1, 2, 3]) >> ListAcc() >> Last()
        print(await sink.get_frame())
        print(await sink.get_frame())


    asyncio.run(main())

.. code-block:: text

    [1, 2, 3]
    [1, 2, 3]

End of the stream
-----------------

A stream ends when its source has no more frames. The end travels down the chain:
each stage finishes after it has passed on everything it holds, and the sink
returns. In an ``async for`` loop you do not see it at all, because the loop just
stops. Underneath, the elements signal the end with
:class:`StopAsyncIteration`, the standard way for an async iterator to say it is
done.

Errors and skipped frames
-------------------------

An exception raised inside a stage travels down to whoever is reading the pipeline,
which is usually your ``await sink.get_frame()`` or your ``async for`` loop. There
you can catch it with a normal ``try`` statement. The elements involved move to the
``ERROR`` state.

A stage can also drop a single frame without failing, by raising
:class:`~pyrill.base.FrameSkipped`. The stream continues with the next frame.
:class:`~pyrill.primitives.SkipIf` does this for you. :doc:`guides/errors-and-retries`
shows both.

Pipelines
---------

A :class:`~pyrill.base.Pipeline` groups the elements that work together. It gives
them a shared logger and a place to send messages. You do not need to create one:
elements without an explicit pipeline join a default one. Create your own when you
want to keep unrelated pipelines apart. An element can only be connected to
elements of the same pipeline:

.. code-block:: python

    from pyrill.base import Pipeline
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource

    source = IterableSource(source=[1], pipeline=Pipeline("one"))
    sink = Last(pipeline=Pipeline("two"))

    try:
        source >> sink
    except RuntimeError as error:
        print(error)

.. code-block:: text

    Cannot connect elements from different pipelines

Messages
--------

A :class:`~pyrill.base.Message` reports an event about an element, such as becoming
ready, starting or exhausting a source, entering an error state, skipping a frame,
or opening a file. Frames flow
through the connected elements; messages go to callbacks registered on the emitting
element and then on its pipeline.

Each message carries the originating element's name (``sender``), the pipeline
name (``pipeline``), an event name (``type``), and event-specific data (``params``).
Register a synchronous callback with ``element.add_message_callback(...)`` for one
element, or ``pipeline.add_message_callback(...)`` for the pipeline. Use this for
monitoring, diagnostics and application progress without consuming frames.
See :doc:`guides/messages-and-monitoring` for runnable examples and event details.

Types
-----

PyRill is fully typed. Elements are generic classes, and the ``>>`` operator is
typed too, so a type checker can verify that each element receives the frames the
previous one produces. Give the type arguments when the checker cannot infer them:

.. code-block:: python3

    source = IterableSource[int](source=[1, 2, 3])
    stage = source >> Map[int, str](map_func=str)    # a stage of int -> str
    sink = stage >> Last[str]()                      # accepts str frames

Connecting elements with mismatched frame types is reported as a type error before
the program runs.

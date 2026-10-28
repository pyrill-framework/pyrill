==========
Quickstart
==========

This page installs PyRill and builds a first pipeline. It takes a few minutes.

Installation
------------

PyRill needs Python 3.14 or later and has no other dependencies.

.. code-block:: console

    $ pip install pyrill

With `uv <https://docs.astral.sh/uv/>`_ use ``uv add pyrill`` instead. To work on
PyRill itself, clone the repository and run ``uv sync``.

Your first pipeline
-------------------

A pipeline starts with a *source*, goes through any number of *stages*, and ends
in a *sink*. You connect the elements with ``>>``:

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        pipeline = (
            IterableSource(source=["alpha", "beta", "gamma"])   # source
            >> Map(map_func=str.upper)                          # stage
            >> ListAcc()                                        # stage
            >> Last()                                           # sink
        )
        print(await pipeline.get_frame())


    asyncio.run(main())

.. code-block:: text

    ['ALPHA', 'BETA', 'GAMMA']

The pieces are:

* :class:`~pyrill.sources.IterableSource` produces the three words, one *frame*
  each.
* :class:`~pyrill.mappers.Map` applies a function to every frame.
* :class:`~pyrill.accumulators.ListAcc` collects the frames received so far into a
  list and emits it after every frame.
* :class:`~pyrill.sinks.Last` waits for the end of the stream and returns the last
  frame, which is the complete list.

``>>`` returns the element on its right, so the whole expression gives you the
sink at the end of the chain. Nothing runs until something reads from it: here,
``get_frame()``.

Getting the results
-------------------

There are three ways to read a pipeline, and you choose by what you need.

**Ask a sink for a value.** :class:`~pyrill.sinks.First` and
:class:`~pyrill.sinks.Last` return one frame with ``await sink.get_frame()``. They
mount the pipeline before reading and unmount it afterwards, so you do not need to
do anything else. The example above uses ``Last``.

**Iterate a stage.** To process every frame yourself, mount the last stage and use
``async for``:

.. code-block:: python

    import asyncio

    from pyrill.mappers import Map
    from pyrill.sources import IterableSource


    async def main() -> None:
        stage = IterableSource(source=[1, 2, 3]) >> Map(map_func=lambda n: n * 10)
        await stage.mount()
        async for frame in stage:
            print(frame)


    asyncio.run(main())

.. code-block:: text

    10
    20
    30

The loop ends by itself when the source runs out of frames, and the pipeline
unmounts.

**Run it for its side effects.** When the point of the pipeline is what the stages
do, for example writing to a file, end it with a sink that discards the frames and
call ``consume_all()``:

.. code-block:: python

    import asyncio

    from pyrill.mappers import Map
    from pyrill.sinks import BlackHole
    from pyrill.sources import IterableSource


    async def main() -> None:
        sink = IterableSource(source=["a", "b"]) >> Map(map_func=print) >> BlackHole()
        await sink.consume_all()


    asyncio.run(main())

.. code-block:: text

    a
    b

Next steps
----------

* :doc:`concepts` explains what elements, frames and the lifecycle are.
* :doc:`guides/index` shows how to read files, parse streams, branch, merge, add
  timing and write your own elements.
* :doc:`catalog/index` lists every element that PyRill provides.

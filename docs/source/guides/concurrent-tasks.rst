================
Concurrent tasks
================

Use :class:`~pyrill.futures.CreateTask` to schedule a coroutine for each frame and
:class:`~pyrill.futures.AsCompleted` to consume the results as they become
available. This lets asynchronous operations overlap while they wait for I/O.
The elements run on the pipeline's event loop; they do not move work to threads
or processes.

Schedule work for each frame
----------------------------

``CreateTask`` calls ``coroutine_builder_func(frame)`` and emits the scheduled
task immediately. The task inherits a copy of the current context. Output tasks
follow input order, even when the work finishes in another order.

This example collects the tasks first, then awaits their results in input order:

.. code-block:: python

    import asyncio

    from pyrill.futures import CreateTask
    from pyrill.sources import IterableSource


    async def double(frame: int) -> int:
        await asyncio.sleep(0.01)
        return frame * 2


    async def main() -> None:
        stage = (
            IterableSource(source=[1, 2, 3])
            >> CreateTask(coroutine_builder_func=double)
        )
        await stage.mount()
        try:
            tasks = [task async for task in stage]
            results = [await task for task in tasks]
            print(results)
        finally:
            await stage.unmount()


    asyncio.run(main())

.. code-block:: text

    [2, 4, 6]

Awaiting a task raises any exception from its coroutine. If the builder itself
raises before returning a coroutine, the exception occurs while reading the
``CreateTask`` stage. Collecting every task retains the whole input in memory;
use the next pattern for a stream of results.

Consume results and limit buffering
-----------------------------------

``AsCompleted`` reads futures in the background and emits their resolved values.
A slow earlier task does not prevent a later completed task from being emitted.
If several futures are already complete, their relative output order is
unspecified.

The :func:`~pyrill.futures.create_task` decorator provides a shorter way to build
the task stage. Use ``@create_task()`` to take the function's name, or pass a
name as below. The decorated variable holds a stage, so connect it with ``>>``.

.. code-block:: python

    import asyncio

    from pyrill.futures import AsCompleted, create_task
    from pyrill.sources import IterableSource


    @create_task("double")
    async def double(frame: int) -> int:
        await asyncio.sleep(0.01)
        return frame * 2


    async def main() -> None:
        stage = (
            IterableSource(source=[1, 2, 3, 4, 5])
            >> double
            >> AsCompleted[int](max_pending=2)
        )
        await stage.mount()
        try:
            results = [result async for result in stage]
            print(sorted(results))
        finally:
            await stage.unmount()


    asyncio.run(main())

.. code-block:: text

    [2, 4, 6, 8, 10]

For a direct call, use ``create_task("double", coroutine_function)`` to obtain a
``CreateTask`` stage without decorating the original function.

``max_pending`` counts both unfinished buffered futures and completed futures
whose results have not been emitted. Set it to a positive integer to pause
upstream consumption when the buffer fills. ``None``, the default, allows
unlimited buffering. The consumer can fetch one additional upstream frame before
waiting to enqueue it, so this buffer limit is not a strict limit on the number
of running tasks. Futures scheduled before entering this pipeline are already
running and cannot be limited by the buffer.

Read the pipeline inside ``try``/``finally`` and unmount it when stopping early.
Unmounting cancels unfinished futures in ``AsCompleted``'s pending buffer.
``CreateTask`` alone does not manage cancellation of the tasks it emits; when
consuming those tasks directly, the caller owns their cleanup. An exception in
a completed future is raised when its result is read from ``AsCompleted``.

Build a reusable task stage
---------------------------

Subclass :class:`~pyrill.futures.BaseCreateTask` when coroutine construction
belongs in an element. Implement ``_create_coroutine(frame)`` as a regular method
that returns a coroutine; the base class schedules it.

.. code-block:: python

    import asyncio
    from collections.abc import Coroutine

    from pyrill.futures import AsCompleted, BaseCreateTask
    from pyrill.sources import IterableSource


    class Stringify(BaseCreateTask[int, str]):
        async def convert(self, frame: int) -> str:
            await asyncio.sleep(0.01)
            return str(frame)

        def _create_coroutine(self, frame: int) -> Coroutine[None, None, str]:
            return self.convert(frame)


    async def main() -> None:
        stage = (
            IterableSource(source=[1, 2, 3])
            >> Stringify()
            >> AsCompleted[str](max_pending=2)
        )
        await stage.mount()
        try:
            results = [result async for result in stage]
            print(sorted(results))
        finally:
            await stage.unmount()


    asyncio.run(main())

.. code-block:: text

    ['1', '2', '3']

See :doc:`../catalog/futures` for the API reference and
:doc:`errors-and-retries` for handling failures in a pipeline.

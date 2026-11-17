=======
Windows
=======

Partition streams by frame count, elapsed time, or a grouping key. Each output
is a producer that can be connected to its own sink.

.. automodule:: pyrill.windows
    :members: BaseWindow, WindowQueue, WindowSize, WindowTime, GroupQueue, GroupBy

See :doc:`../guides/windowing` for collecting batches, computing per-window
totals, creating timed windows, and reading group keys from grouped sources.

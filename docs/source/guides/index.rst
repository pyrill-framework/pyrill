======
Guides
======

These guides each solve a common task. Every example is complete: copy it, run it
and change it.

.. toctree::
    :maxdepth: 1

    files-and-text
    parsing-and-converting
    branching-and-merging
    timing-and-flow
    custom-elements
    errors-and-retries

:doc:`files-and-text`
    Read and write files in chunks, split them into lines and filter them, with
    constant memory.

:doc:`parsing-and-converting`
    Rebuild records from byte streams, decode text safely, and convert between CSV
    and JSON.

:doc:`branching-and-merging`
    Send a stream to several consumers, pair frames from several streams and merge
    live feeds.

:doc:`timing-and-flow`
    Throttle, set deadlines, start and stop at a given moment, pause and resume, and
    push frames in from your own code.

:doc:`custom-elements`
    Write your own sources, stages and sinks, and package a chain of stages as one.

:doc:`errors-and-retries`
    Understand what happens when something fails, drop bad frames and retry.

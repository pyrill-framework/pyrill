================
Elements catalog
================

Element descriptions include **Consumes** and **Produces** fields inferred from
their generic base classes. A source has a produced frame type, a sink has a
consumed frame type, and a stage has both. Unbound parameters such as
``Source_co`` represent types chosen when the element is specialized; nested
types such as ``list[Source_co]`` describe the complete output frame.

See :doc:`../guides/sphinx` to enable these fields in your own documentation.


.. toctree::
    :maxdepth: 2

    base
    sources
    sinks
    primitives
    mappers
    strlike
    strings
    bytes
    chunks
    lists
    accumulators
    csv
    json
    io
    queues
    windows
    futures
    time
    debug

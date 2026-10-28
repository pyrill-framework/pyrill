====
Core
====

The core abstractions every element is built from: the pipeline that owns the
elements, the element lifecycle (``NULL``, ``READY`` and ``ERROR`` states), and
the producer, consumer, source, stage and sink base classes. Subclass these to
write custom elements.

.. automodule:: pyrill.base

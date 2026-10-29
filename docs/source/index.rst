Welcome to PyRill's documentation!
==================================

.. image:: /_static/brand/social-banner.png
   :alt: PyRill — Asynchronous streams for Python
   :width: 100%

PyRill builds asynchronous data pipelines in plain Python. You connect small
elements with ``>>`` and the data flows through them one frame at a time, on top
of :mod:`asyncio` and with no dependencies.

.. code-block:: python

    import asyncio

    from pyrill.accumulators import ListAcc
    from pyrill.mappers import Map
    from pyrill.sinks import Last
    from pyrill.sources import IterableSource


    async def main() -> None:
        pipeline = (
            IterableSource(source=["alpha", "beta", "gamma"])
            >> Map(map_func=str.upper)
            >> ListAcc()
            >> Last()
        )
        print(await pipeline.get_frame())


    asyncio.run(main())

.. code-block:: text

    ['ALPHA', 'BETA', 'GAMMA']

New to PyRill? Read :doc:`overview` to see what it is for, then follow the
:doc:`quickstart`. The :doc:`concepts` page explains the ideas behind the
library, and the :doc:`guides <guides/index>` show how to solve common tasks.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   overview
   quickstart
   concepts
   guides/index
   catalog/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

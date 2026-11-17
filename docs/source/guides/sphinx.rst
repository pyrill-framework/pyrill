===========================
Document elements in Sphinx
===========================

The ``pyrill.sphinx`` extension adds **Consumes** and **Produces** fields to
autodoc descriptions of Pyrill elements. It resolves their declared frame types
and links known classes and type aliases to their documentation.

Enable the extension
--------------------

Install Pyrill and Sphinx in the environment used to build your documentation:

.. code-block:: console

    python -m pip install pyrill sphinx

The extension ships with Pyrill; no separate plugin package is needed. When
working in the Pyrill repository, install the documentation dependencies with
``uv sync --group docs`` instead.

Add the extension to your existing ``extensions`` list in ``conf.py``:

.. code-block:: python

    extensions = [
        "sphinx.ext.autodoc",
        "sphinx.ext.intersphinx",
        "pyrill.sphinx",
    ]

    intersphinx_mapping = {
        "python": ("https://docs.python.org/3/", None),
    }

``pyrill.sphinx`` loads autodoc automatically. Intersphinx is needed to resolve
links to classes documented by other projects, including Python's built-in
types. Keep any other extensions and inventory mappings your project uses.

Document an element
-------------------

Use the usual autodoc directives in your reStructuredText pages:

.. code-block:: rst

    .. autoclass:: pyrill.bytes.Decode

The description gains **Consumes: bytes** and **Produces: str**, with links to
Python's documentation. To document every public class in a module, use:

.. code-block:: rst

    .. automodule:: pyrill.accumulators
        :members:

Build your documentation normally, for example:

.. code-block:: console

    sphinx-build -W -b html docs/source docs/build

The documented modules must be importable in the build environment. The
extension inspects classes and annotations without constructing elements or
running pipelines.

Understand the generated fields
-------------------------------

The fields describe the public frame types declared by each element:

.. list-table::
    :header-rows: 1

    * - Element base
      - Generated fields
    * - ``BaseProducer`` or ``BaseParallelProducer``
      - Produces
    * - ``BaseConsumer``
      - Consumes
    * - ``BaseStage``
      - Consumes and Produces

For example, put this class in ``yourpackage/elements.py``:

.. code-block:: python

    from pyrill.base import BaseStage


    class Numbers(BaseStage[str, list[int]]):
        """Convert comma-separated text into one list of integers per frame."""

        async def _process_frame(self, frame: str) -> list[int]:
            return [int(part) for part in frame.split(",")]

Then document it with:

.. code-block:: rst

    .. autoclass:: yourpackage.elements.Numbers

Its fields show **Consumes: str** and **Produces: list[int]**. Classes within
nested types link too.

Generic inheritance is followed through intermediate classes. Unbound type
parameters keep their declared names: ``ListAcc`` consumes ``Source_co`` and
produces ``list[Source_co]``. A subclass of ``ListAcc[str]`` instead consumes
``str`` and produces ``list[str]``. The extension does not infer frame types
from constructor arguments or runtime values.

Document decorated stages
-------------------------

Stages created with :func:`~pyrill.mappers.make_map` use the wrapped function's
annotations. For example:

.. code-block:: python

    from pyrill.mappers import make_map


    @make_map
    def Label(frame: int, /, *, prefix: str = "item-") -> str:
        """Add a prefix to an integer frame."""
        return f"{prefix}{frame}"

Document ``Label`` with ``autoclass`` just like any other stage. It consumes
``int`` and produces ``str``. The first positional parameter describes the
input frame; configuration parameters such as ``prefix`` do not change that
type.

For an asynchronous function, the return annotation describes the produced
frame. For a synchronous function returning ``Awaitable[T]`` or
``Coroutine[..., T]``, the produced type is the awaited result ``T``. Missing
annotations fall back to the generic base types.

Link classes and type aliases
-----------------------------

Classes documented in your Sphinx project resolve locally. For external
classes, add their project's inventory to ``intersphinx_mapping``. If a target
is unavailable, its type name still appears, without a resolved link.

Named aliases retain their name and link to their definition. For example,
:class:`~pyrill.json.FromJson` produces :type:`~pyrill.json.JSONValue`. Include
the alias in your API documentation so that Sphinx can resolve it:

.. code-block:: rst

    .. automodule:: pyrill.json
        :members: FromJson, JSONValue
        :undoc-members:

The Python mapping above downloads Python's inventory during the build. For
offline builds, replace ``None`` with the path to a downloaded inventory file.

See :doc:`custom-elements` for writing elements and the
:doc:`element catalog <../catalog/index>` for generated descriptions of
Pyrill's built-in elements.

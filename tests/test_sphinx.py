from collections.abc import Awaitable, Coroutine
from importlib.metadata import version
from io import StringIO
from pathlib import Path
from re import DOTALL, findall
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock
from zlib import compress

import pytest

from pyrill.accumulators import ListAcc
from pyrill.base import BaseConsumer, BaseParallelProducer, BaseProducer, BaseStage
from pyrill.bytes import Decode
from pyrill.io import StreamReaderSink, StreamReaderSource
from pyrill.json import FromJson, JSONValue
from pyrill.mappers import make_map
from pyrill.sphinx import (
    ElementFrameTypes,
    add_element_frame_types,
    get_element_frame_types,
    setup,
)
from pyrill.strings import Encode, Lower, Upper
from pyrill.windows import WindowSize

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.ext.autodoc import Options


class ByteSource(BaseProducer[bytes]):
    """Example source."""


class StringConsumer(BaseConsumer[str]):
    """Example consumer."""


class ParallelSource(BaseParallelProducer[int]):
    """Example parallel producer."""


class Batch[T](BaseStage[T, list[T]]):
    """Example generic stage."""


class IntBatch(Batch[int]):
    """Example specialized stage."""


class InheritedBatch(IntBatch):
    """Example subclass without its own generic bases."""


class NestedBatch(Batch[tuple[str, int | None]]):
    """Example stage with a nested input type."""


class UnrelatedMixin:
    """Example mixin with no frame types."""


class MixedBatch(UnrelatedMixin, IntBatch):
    """Example stage with multiple inheritance."""


class Frame:
    """Example frame with a local API reference."""


class FrameSource(BaseProducer[Frame]):
    """Source of locally documented frames."""


class JsonBatch(BaseStage[JSONValue, list[JSONValue]]):
    """Example stage with a type alias inside a generic type."""


@make_map
def FormatFrame(frame: int, /, *, prefix: str = "") -> str:
    return f"{prefix}{frame}"


@make_map
async def AsyncFormat(frame: int, /) -> list[str]:
    return [str(frame)]


@make_map
def AwaitableFormat(frame: int, /) -> Awaitable[str]:
    async def convert() -> str:
        return str(frame)

    return convert()


@make_map
def CoroutineFormat(frame: int, /) -> Coroutine[None, None, str]:
    async def convert() -> str:
        return str(frame)

    return convert()


@make_map
def MixedFormat(frame: int, /) -> str | Awaitable[str]:
    return str(frame)


@make_map
def ForwardFormat(frame: int, /) -> list[str]:
    return [str(frame)]


@make_map
def GenericBatch[T](frame: T, /) -> list[T]:
    return [frame]


@make_map
def Uncalled(frame: int, /) -> str:
    raise AssertionError("Documentation must not execute the mapping function")


class TestElementFrameTypes:
    @pytest.mark.parametrize(
        ("element", "expected"),
        [
            (ByteSource, ElementFrameTypes(produces="bytes")),
            (StringConsumer, ElementFrameTypes(consumes="str")),
            (ParallelSource, ElementFrameTypes(produces="int")),
            (BaseProducer, ElementFrameTypes(produces="Source_co")),
            (BaseConsumer, ElementFrameTypes(consumes="Sink_co")),
            (Batch, ElementFrameTypes("T", "list[T]")),
            (IntBatch, ElementFrameTypes("int", "list[int]")),
            (InheritedBatch, ElementFrameTypes("int", "list[int]")),
            (MixedBatch, ElementFrameTypes("int", "list[int]")),
            (
                NestedBatch,
                ElementFrameTypes(
                    "tuple[str, int | None]", "list[tuple[str, int | None]]"
                ),
            ),
            (ListAcc, ElementFrameTypes("Source_co", "list[Source_co]")),
            (StreamReaderSource, ElementFrameTypes(produces="bytes")),
            (StreamReaderSink, ElementFrameTypes(consumes="bytes")),
            (FromJson, ElementFrameTypes("str", "JSONValue")),
            (Upper, ElementFrameTypes("str", "str")),
            (Lower, ElementFrameTypes("str", "str")),
            (Decode, ElementFrameTypes("bytes", "str")),
            (Encode, ElementFrameTypes("str", "bytes")),
            (FormatFrame, ElementFrameTypes("int", "str")),
            (AsyncFormat, ElementFrameTypes("int", "list[str]")),
            (AwaitableFormat, ElementFrameTypes("int", "str")),
            (CoroutineFormat, ElementFrameTypes("int", "str")),
            (MixedFormat, ElementFrameTypes("int", "str")),
            (ForwardFormat, ElementFrameTypes("int", "list[str]")),
            (GenericBatch, ElementFrameTypes("T", "list[T]")),
            (Uncalled, ElementFrameTypes("int", "str")),
            (
                WindowSize,
                ElementFrameTypes("Source_co", "BaseProducer[Source_co]"),
            ),
            (UnrelatedMixin, ElementFrameTypes()),
        ],
    )
    def test_resolves_inherited_frame_types(
        self, element: type[Any], expected: ElementFrameTypes
    ) -> None:
        result = get_element_frame_types(element)

        assert result == expected


class TestAutodocFrameTypes:
    @pytest.mark.parametrize(
        ("element", "fields"),
        [
            (ByteSource, [":Produces: :py:class:`bytes <bytes>`"]),
            (StringConsumer, [":Consumes: :py:class:`str <str>`"]),
            (
                IntBatch,
                [
                    ":Consumes: :py:class:`int <int>`",
                    ":Produces: :py:class:`list <list>`\\ [:py:class:`int <int>`\\ ]",
                ],
            ),
            (
                FrameSource,
                [":Produces: :py:class:`Frame <tests.test_sphinx.Frame>`"],
            ),
            (
                Batch,
                [":Consumes: ``T``", ":Produces: :py:class:`list <list>`\\ [``T``\\ ]"],
            ),
            (
                FromJson,
                [
                    ":Consumes: :py:class:`str <str>`",
                    ":Produces: :py:type:`JSONValue <pyrill.json.JSONValue>`",
                ],
            ),
            (
                JsonBatch,
                [
                    ":Consumes: :py:type:`JSONValue <pyrill.json.JSONValue>`",
                    (
                        ":Produces: :py:class:`list <list>`\\ ["
                        ":py:type:`JSONValue <pyrill.json.JSONValue>`\\ ]"
                    ),
                ],
            ),
            (
                Decode,
                [
                    ":Consumes: :py:class:`bytes <bytes>`",
                    ":Produces: :py:class:`str <str>`",
                ],
            ),
        ],
    )
    def test_class_description_includes_fields_and_preserves_docstring(
        self, element: type[Any], fields: list[str]
    ) -> None:
        app = cast("Sphinx", MagicMock())
        options = cast("Options", MagicMock())
        lines = ["Original description.", "", "More detail."]

        add_element_frame_types(app, "class", element.__name__, element, options, lines)

        assert lines == [*fields, "", "Original description.", "", "More detail."]

    @pytest.mark.parametrize(
        ("what", "obj"),
        [("module", ByteSource), ("function", len), ("class", UnrelatedMixin)],
    )
    def test_unrelated_descriptions_are_unchanged(self, what: str, obj: object) -> None:
        app = cast("Sphinx", MagicMock())
        options = cast("Options", MagicMock())
        lines = ["Original description."]

        add_element_frame_types(app, what, "example", obj, options, lines)

        assert lines == ["Original description."]

    def test_setup_registers_autodoc_hook(self) -> None:
        app = MagicMock()

        metadata = setup(app)

        app.setup_extension.assert_called_once_with("sphinx.ext.autodoc")
        app.connect.assert_called_once_with(
            "autodoc-process-docstring", add_element_frame_types
        )
        assert metadata == {
            "version": version("pyrill"),
            "parallel_read_safe": True,
            "parallel_write_safe": True,
        }

    def test_sphinx_build_renders_element_frame_fields(self, tmp_path: Path) -> None:
        pytest.importorskip("sphinx")
        from docutils import nodes
        from sphinx.application import Sphinx

        source = tmp_path / "source"
        source.mkdir()
        inventory = tmp_path / "python.inv"
        inventory.write_bytes(
            b"# Sphinx inventory version 2\n"
            b"# Project: Python\n"
            b"# Version: 3\n"
            b"# The remainder of this file is compressed using zlib.\n"
            + compress(
                b"bytes py:class 1 library/stdtypes.html#bytes -\n"
                b"str py:class 1 library/stdtypes.html#str -\n"
                b"int py:class 1 library/functions.html#int -\n"
                b"list py:class 1 library/stdtypes.html#list -\n"
            )
        )
        (source / "conf.py").write_text(
            'extensions = ["pyrill.sphinx", "sphinx.ext.intersphinx"]\n'
            f'intersphinx_mapping = {{"python": ("https://docs.python.org/3/", {str(inventory)!r})}}\n',
            encoding="utf-8",
        )
        (source / "index.rst").write_text(
            "Elements\n========\n\n"
            ".. autoclass:: tests.test_sphinx.ByteSource\n\n"
            ".. autoclass:: tests.test_sphinx.StringConsumer\n\n"
            ".. autoclass:: tests.test_sphinx.IntBatch\n\n"
            ".. autoclass:: tests.test_sphinx.Frame\n\n"
            ".. autoclass:: tests.test_sphinx.FrameSource\n\n"
            ".. autoclass:: pyrill.bytes.Decode\n\n"
            ".. autoclass:: pyrill.strings.Upper\n\n"
            ".. automodule:: pyrill.json\n"
            "    :members: FromJson, JSONValue\n"
            "    :undoc-members:\n",
            encoding="utf-8",
        )
        warnings = StringIO()
        app = Sphinx(
            srcdir=str(source),
            confdir=str(source),
            outdir=str(tmp_path / "html"),
            doctreedir=str(tmp_path / "doctrees"),
            buildername="html",
            status=StringIO(),
            warning=warnings,
            freshenv=True,
            warningiserror=True,
        )

        app.build()

        html = (tmp_path / "html" / "index.html").read_text(encoding="utf-8")
        assert app.statuscode == 0, warnings.getvalue()
        assert warnings.getvalue() == ""
        fields = [
            field.astext()
            for field in app.env.get_doctree("index").findall(nodes.field_name)
        ]
        assert fields.count("Consumes") == 5
        assert fields.count("Produces") == 6
        assert 'href="https://docs.python.org/3/library/stdtypes.html#bytes"' in html
        assert 'href="https://docs.python.org/3/library/stdtypes.html#str"' in html
        assert 'href="https://docs.python.org/3/library/functions.html#int"' in html
        assert 'href="https://docs.python.org/3/library/stdtypes.html#list"' in html
        assert 'href="#tests.test_sphinx.Frame"' in html
        assert "Example specialized stage." in html
        produced_fields = findall(
            r'<dt[^>]*>Produces<span class="colon">:</span></dt>\s*<dd[^>]*>(.*?)</dd>',
            html,
            flags=DOTALL,
        )
        assert any(
            'href="#pyrill.json.JSONValue"' in field for field in produced_fields
        )
        decode = html.split('id="pyrill.bytes.Decode"', 1)[1].split(
            'id="pyrill.strings.Upper"', 1
        )[0]
        assert 'href="https://docs.python.org/3/library/stdtypes.html#bytes"' in decode
        assert 'href="https://docs.python.org/3/library/stdtypes.html#str"' in decode
        assert "Sink_co" not in decode
        assert "Source_co" not in decode

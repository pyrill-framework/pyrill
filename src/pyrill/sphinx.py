"""Sphinx extension that documents the frame types of PyRill elements.

Add ``"pyrill.sphinx"`` to ``extensions`` in a Sphinx configuration. Autodoc
class descriptions then include Consumes and Produces fields, resolved from
the element's generic producer and consumer bases without instantiating it.
"""

from collections.abc import Awaitable, Coroutine, Mapping
from dataclasses import dataclass
from importlib import metadata
from inspect import Parameter, get_annotations, iscoroutinefunction, signature, unwrap
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    ForwardRef,
    ParamSpec,
    TypeAliasType,
    TypeVar,
    TypeVarTuple,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from .base import BaseConsumer, BaseParallelProducer, BaseProducer

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.ext.autodoc import Options


@dataclass(frozen=True)
class ElementFrameTypes:
    """Frame type descriptions for a producer, consumer, or stage."""

    consumes: str | None = None
    produces: str | None = None


def _format_type(
    annotation: Any, bindings: Mapping[object, str], link_classes: bool
) -> str:
    def literal(value: str) -> str:
        return f"``{value}``" if link_classes else value

    if isinstance(annotation, (TypeVar, ParamSpec, TypeVarTuple)):
        return bindings.get(annotation, literal(annotation.__name__))
    if isinstance(annotation, TypeAliasType):
        if link_classes:
            target = f"{annotation.__module__}.{annotation.__name__}"
            return f":py:type:`{annotation.__name__} <{target}>`"
        return annotation.__name__
    if annotation is None or annotation is type(None):
        return literal("None")
    if annotation is Ellipsis:
        return literal("...")
    if isinstance(annotation, ForwardRef):
        return literal(annotation.__forward_arg__)
    if isinstance(annotation, str):
        return literal(annotation)
    if isinstance(annotation, list):
        parameters = cast(list[object], annotation)
        return f"[{', '.join(_format_type(arg, bindings, link_classes) for arg in parameters)}]"

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin in (Union, UnionType):
        return " | ".join(
            _format_type(arg, bindings, link_classes) for arg in arguments
        )
    if origin is not None:
        name = _format_type(origin, bindings, link_classes)
        args = ", ".join(_format_type(arg, bindings, link_classes) for arg in arguments)
        # Escaped spaces separate reST inline roles without adding visible gaps.
        return f"{name}\\ [{args}\\ ]" if link_classes else f"{name}[{args}]"

    if isinstance(annotation, type):
        if link_classes:
            target = (
                annotation.__qualname__
                if annotation.__module__ == "builtins"
                else f"{annotation.__module__}.{annotation.__qualname__}"
            )
            return f":py:class:`{annotation.__qualname__} <{target}>`"
        return annotation.__qualname__
    return literal(str(annotation).removeprefix("typing."))


def get_element_frame_types(
    element: type[Any], *, link_classes: bool = False
) -> ElementFrameTypes:
    """Resolve consumed and produced types through an element's generic bases.

    Unbound type parameters retain their declared names. When several paths
    reach a producer or consumer base, the first path in base-class order wins.
    With ``link_classes=True``, render known classes as fully qualified Sphinx
    cross-references and unbound parameters as inline literals.
    """
    resolved: dict[str, str] = {}

    def visit(cls: type[Any], bindings: Mapping[object, str]) -> None:
        wrapped_map = cls.__dict__.get("_map_frame")
        if callable(wrapped_map) and hasattr(wrapped_map, "__wrapped__"):
            function = unwrap(wrapped_map)
            try:
                annotations = get_type_hints(function)
            except NameError, TypeError:
                annotations = get_annotations(function, eval_str=False)
            frame_parameter = next(
                (
                    parameter
                    for parameter in signature(function).parameters.values()
                    if parameter.kind
                    in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
                ),
                None,
            )
            if frame_parameter is not None and frame_parameter.name in annotations:
                resolved.setdefault(
                    "consumes",
                    _format_type(
                        annotations[frame_parameter.name], bindings, link_classes
                    ),
                )
            if "return" in annotations:
                annotation = annotations["return"]
                result = (
                    _format_type(annotation, bindings, link_classes)
                    if iscoroutinefunction(function)
                    else _format_awaited_type(annotation, bindings, link_classes)
                )
                resolved.setdefault("produces", result)

        parameters = cast(tuple[object, ...], getattr(cls, "__parameters__", ()))
        if cls in (BaseProducer, BaseParallelProducer, BaseConsumer):
            field = "consumes" if cls is BaseConsumer else "produces"
            frame = (
                _format_type(parameters[0], bindings, link_classes)
                if parameters
                else _format_type(Any, bindings, link_classes)
            )
            resolved.setdefault(field, frame)
            return

        # Read the class's own bases: getattr would reuse a parent's stale
        # __orig_bases__ for subclasses that introduce no new type arguments.
        bases = cast(
            tuple[object, ...], cls.__dict__.get("__orig_bases__", cls.__bases__)
        )
        for base in bases:
            origin = get_origin(base) or base
            if not isinstance(origin, type) or not issubclass(
                origin, (BaseProducer, BaseParallelProducer, BaseConsumer)
            ):
                continue
            base_class = cast(type[Any], origin)
            base_parameters = cast(
                tuple[object, ...], getattr(base_class, "__parameters__", ())
            )
            arguments = get_args(base)
            next_bindings = dict(bindings)
            next_bindings.update(
                (parameter, _format_type(argument, bindings, link_classes))
                for parameter, argument in zip(base_parameters, arguments)
            )
            visit(base_class, next_bindings)

    visit(element, {})
    return ElementFrameTypes(resolved.get("consumes"), resolved.get("produces"))


def _format_awaited_type(
    annotation: Any, bindings: Mapping[object, str], link_classes: bool
) -> str:
    origin = get_origin(annotation)
    if origin in (Awaitable, Coroutine):
        return _format_type(get_args(annotation)[-1], bindings, link_classes)
    if origin in (Union, UnionType):
        results = dict.fromkeys(
            _format_awaited_type(argument, bindings, link_classes)
            for argument in get_args(annotation)
        )
        return " | ".join(results)
    return _format_type(annotation, bindings, link_classes)


def add_element_frame_types(
    app: Sphinx,
    what: str,
    name: str,
    obj: object,
    options: Options,
    lines: list[str],
) -> None:
    """Add frame type fields to autodoc class descriptions."""
    if what != "class" or not isinstance(obj, type):
        return
    frame_types = get_element_frame_types(obj, link_classes=True)
    fields: list[str] = []
    if frame_types.consumes is not None:
        fields.append(f":Consumes: {frame_types.consumes}")
    if frame_types.produces is not None:
        fields.append(f":Produces: {frame_types.produces}")
    if fields:
        lines[:0] = [*fields, ""]


def setup(app: Sphinx) -> dict[str, str | bool]:
    """Register the extension with Sphinx."""
    app.setup_extension("sphinx.ext.autodoc")
    app.connect("autodoc-process-docstring", add_element_frame_types)
    return {
        "version": metadata.version("pyrill"),
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }

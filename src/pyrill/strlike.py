from collections.abc import Iterable
from typing import Any, cast

from .mappers import BaseMap

__all__ = ["Join", "LStrip", "RStrip", "Replace", "Split", "Strip"]

# These mappers are generic over str/bytes, so they are written as classes:
# make_map cannot keep a generic function generic for mypy.


class Split[AnyStr: (str, bytes)](BaseMap[AnyStr, list[AnyStr]]):
    """
    Stage that splits each ``str`` or ``bytes`` frame into a list of parts.

    It applies ``frame.split(sep=sep, maxsplit=maxsplit)`` to every frame, so the output
    frame is a list of the same type as the input (``list[str]`` or ``list[bytes]``).

    :param sep: Separator to split on, of the same type as the frames. When ``None``
        (default), split on runs of whitespace.
    :param maxsplit: Maximum number of splits. ``-1`` (default) means no limit.

    Input and output::

        input:  "a,b,c"
        output: ["a", "b", "c"]

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import Split

        source = IterableSource(source=["a,b,c", "d,e"])
        stage = source >> Split(sep=",", maxsplit=1)

        await stage.mount()
        frames = [frame async for frame in stage]
        # [["a", "b,c"], ["d", "e"]]
    """

    def __init__(
        self,
        *args: Any,
        sep: AnyStr | None = None,
        maxsplit: int = -1,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.sep: AnyStr | None = sep
        self.maxsplit = maxsplit

    def _map_frame(self, frame: AnyStr) -> list[AnyStr]:
        # Needed by pyright; mypy checks each constraint and sees it as redundant.
        return cast(  # type: ignore[redundant-cast]
            list[AnyStr], frame.split(sep=self.sep, maxsplit=self.maxsplit)
        )


class Join[AnyStr: (str, bytes)](BaseMap[Iterable[AnyStr], AnyStr]):
    """
    Stage that joins each frame (an iterable of ``str`` or ``bytes``) into one value.

    It applies ``join_str.join(frame)`` to every frame. It is the inverse of
    :class:`Split`. A frame is processed with an error (and the stage goes to
    ``ERROR``) when ``join_str`` was not set, because the stage raises
    :class:`RuntimeError` with ``"Join string must be set"``.

    :param join_str: Separator placed between the parts, of the same type as the parts.
        Required in practice: the default ``None`` makes every frame fail.

    Input and output::

        input:  ["a", "b", "c"]
        output: "a-b-c"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import Join

        source = IterableSource(source=[["a", "b", "c"], ["d", "e"]])
        stage = source >> Join(join_str="-")

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["a-b-c", "d-e"]
    """

    def __init__(
        self, *args: Any, join_str: AnyStr | None = None, **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)

        self.join_str: AnyStr | None = join_str

    def _map_frame(self, frame: Iterable[AnyStr]) -> AnyStr:
        if self.join_str is None:
            raise RuntimeError("Join string must be set")
        return self.join_str.join(frame)


class Replace[AnyStr: (str, bytes)](BaseMap[AnyStr, AnyStr]):
    """
    Stage that replaces substrings in each ``str`` or ``bytes`` frame.

    It applies ``frame.replace(old, new, count)`` to every frame. When ``old`` or ``new``
    is ``None`` it is treated as an empty value of the frame type, so ``new=None``
    deletes every occurrence of ``old``.

    :param old: Substring to look for, of the same type as the frames.
    :param new: Replacement, of the same type as the frames.
    :param count: Maximum number of replacements per frame. ``-1`` (default) replaces
        all occurrences.

    Input and output::

        input:  "a-b-c"
        output: "a_b_c"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import Replace

        source = IterableSource(source=["a-b-c", "x-y"])
        stage = source >> Replace(old="-", new="_", count=1)

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["a_b-c", "x_y"]
    """

    def __init__(
        self,
        *args: Any,
        old: AnyStr | None = None,
        new: AnyStr | None = None,
        count: int = -1,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.old: AnyStr | None = old
        self.new: AnyStr | None = new
        self.count = count

    def _map_frame(self, frame: AnyStr) -> AnyStr:
        old = frame[:0] if self.old is None else self.old
        new = frame[:0] if self.new is None else self.new
        return frame.replace(old, new, self.count)


class BaseStripMap[AnyStr: (str, bytes)](BaseMap[AnyStr, AnyStr]):
    """
    Base class of :class:`Strip`, :class:`LStrip` and :class:`RStrip`.

    It stores the ``chars`` constructor keyword in ``self.chars`` (``None`` means
    whitespace). A subclass implements ``_map_frame(self, frame)`` and applies the
    matching ``str``/``bytes`` method (``strip``, ``lstrip`` or ``rstrip``) with
    ``self.chars``.

    :param chars: Characters to remove, of the same type as the frames. ``None``
        (default) removes whitespace.

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import BaseStripMap

        class StripLower(BaseStripMap[str]):
            def _map_frame(self, frame: str) -> str:
                return frame.strip(self.chars).lower()

        source = IterableSource(source=["--Ab--", "-C-"])
        stage = source >> StripLower(chars="-")

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["ab", "c"]
    """

    def __init__(self, *args: Any, chars: AnyStr | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.chars: AnyStr | None = chars


class Strip[AnyStr: (str, bytes)](BaseStripMap[AnyStr]):
    """
    Stage that removes leading and trailing characters of each ``str``/``bytes`` frame.

    It applies ``frame.strip(chars)`` to every frame.

    :param chars: Characters to remove, of the same type as the frames. ``None``
        (default) removes whitespace.

    Input and output::

        input:  "  hi  "
        output: "hi"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import Strip

        source = IterableSource(source=[b"xxhixx", b"xyx"])
        stage = source >> Strip(chars=b"x")

        await stage.mount()
        frames = [frame async for frame in stage]
        # [b"hi", b"y"]
    """

    def _map_frame(self, frame: AnyStr) -> AnyStr:
        return frame.strip(self.chars)


class RStrip[AnyStr: (str, bytes)](BaseStripMap[AnyStr]):
    """
    Stage that removes trailing characters of each ``str``/``bytes`` frame.

    It applies ``frame.rstrip(chars)`` to every frame.

    :param chars: Characters to remove, of the same type as the frames. ``None``
        (default) removes whitespace.

    Input and output::

        input:  "line\\n"
        output: "line"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import RStrip

        source = IterableSource(source=["a\\n", "b  \\n"])
        stage = source >> RStrip()

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["a", "b"]
    """

    def _map_frame(self, frame: AnyStr) -> AnyStr:
        return frame.rstrip(self.chars)


class LStrip[AnyStr: (str, bytes)](BaseStripMap[AnyStr]):
    """
    Stage that removes leading characters of each ``str``/``bytes`` frame.

    It applies ``frame.lstrip(chars)`` to every frame.

    :param chars: Characters to remove, of the same type as the frames. ``None``
        (default) removes whitespace.

    Input and output::

        input:  "00042"
        output: "42"

    Usage:

    .. code-block:: python

        from pyrill.sources import IterableSource
        from pyrill.strlike import LStrip

        source = IterableSource(source=["00042", "007"])
        stage = source >> LStrip(chars="0")

        await stage.mount()
        frames = [frame async for frame in stage]
        # ["42", "7"]
    """

    def _map_frame(self, frame: AnyStr) -> AnyStr:
        return frame.lstrip(self.chars)

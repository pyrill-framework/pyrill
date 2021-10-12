from typing import AnyStr, Iterable, List

from .mappers import make_map

__all__ = ['Split', 'Join', 'Replace', 'Strip', 'RStrip', 'LStrip']


@make_map
def Split(frame: AnyStr, *, sep: AnyStr = None, maxsplit: int = -1, **kwargs) -> List[AnyStr]:
    return frame.split(sep=sep, maxsplit=maxsplit)


@make_map
def Join(frame: Iterable[AnyStr], *, join_str: AnyStr = None, **kwargs) -> AnyStr:
    if join_str is None:
        raise RuntimeError('Join string must be set')
    return join_str.join(frame)


@make_map
def Replace(frame: AnyStr, *, old: AnyStr = None, new: AnyStr = None, count: int = -1, **kwargs) -> AnyStr:
    if old is None:
        old = frame[:0]
    if new is None:
        new = frame[:0]
    return frame.replace(old, new, count)


@make_map
def Strip(frame: AnyStr, *, chars: AnyStr = None, **kwargs) -> AnyStr:
    return frame.strip(chars)


@make_map
def RStrip(frame: AnyStr, *, chars: AnyStr = None, **kwargs) -> AnyStr:
    return frame.rstrip(chars)


@make_map
def LStrip(frame: AnyStr, *, chars: AnyStr = None, **kwargs) -> AnyStr:
    return frame.lstrip(chars)

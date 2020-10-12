#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

import dataclasses
from typing import Type, TypeVar

T = TypeVar('T')


def dataclass_slots(cls: Type[T]) -> Type[T]:
    """Converts a dataclass into a tightly defined class by
    removing the generic __dict__ and replacing it with __slots__.

    This is beneficial for tiny classes that get created rapidly.
    """

    if '__slots__' in cls.__dict__:
        raise TypeError(f'{cls.__name__} already has __slots__')
    # create a copy of the class dictionary
    cls_dict = dict(cls.__dict__)
    # extract the fields
    fields = tuple(f.name for f in dataclasses.fields(cls))
    # create a __slots__
    cls_dict['__slots__'] = fields
    # remove each attribute, referenced in _MARKER
    for f in fields:
        cls_dict.pop(f, None)
    # remove the __dict__ itself
    cls_dict.pop('__dict__', None)
    # create a new class
    qualname = getattr(cls, '__qualname__', None)
    cls = type(cls)(cls.__name__, cls.__bases__, cls_dict)
    if qualname is not None:  # pragma: no cover
        cls.__qualname__ = qualname
    return cls

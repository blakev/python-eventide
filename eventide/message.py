#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from uuid import UUID
from functools import total_ordering
from dataclasses import field, dataclass
from typing import Optional

from eventide.types import JSON
from eventide.utils import dataclass_slots
from eventide.constants import DEFAULT_GEN_UUID


@dataclass_slots
@dataclass(frozen=True, repr=True)
@total_ordering
class Message:
    """Base Message Implementation"""

    stream: str
    type: str
    data: JSON
    id: Optional[UUID] = field(default_factory=DEFAULT_GEN_UUID)
    metadata: Optional[JSON] = field(default=None)
    position: Optional[int] = field(default=0)

    def _is_valid(self, other):
        return hasattr(other, 'position')

    def __gt__(self, other):
        if not self._is_valid(other):
            raise NotImplementedError
        return self.position > other.position

    def __ge__(self, other):
        if not self._is_valid(other):
            raise NotImplementedError
        return self.position >= other.position

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            raise NotImplementedError
        return self.stream == other.stream \
            and self.type == other.type \
            and self.data == other.data \
            and self.metadata == other.metadata

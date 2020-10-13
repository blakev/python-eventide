#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from uuid import UUID
from datetime import datetime
from functools import total_ordering
from dataclasses import field, dataclass
from typing import Any, Optional

import pendulum
from pendulum import DateTime

from eventide.types import JSON
from eventide.utils import dataclass_slots
from eventide.constants import DEFAULT_GEN_UUID, DEFAULT_JSON_LOADS as jloads

__all__ = [
    'Stream',
    'Message',
]


class Stream:
    def __init__(self, name: str):
        self._name = name

    def __str__(self) -> str:
        return self._name


@dataclass_slots
@dataclass(frozen=True, repr=True)
@total_ordering
class Message:
    """Base Message Implementation"""

    stream: str
    type: str
    data: JSON
    id: Optional[UUID] = field(default_factory=DEFAULT_GEN_UUID)
    metadata: Optional[JSON] = field(default_factory=dict)
    position: Optional[int] = field(default=0)
    time: Optional[DateTime] = field(default=None)

    @classmethod
    def from_row(cls, row: Any) -> 'Message':
        """Optimized method for returning a new Message instance when the
        row Record is the result from calling a stored procedure or function.

        This method assumes that the data will need to be de-serialized (json loaded)
         before it can be used by the application.
        """
        return cls(
            row.stream_name,
            row.type,
            jloads(row.data),
            UUID(row.id),
            jloads(row.metdata) if row.metadata else {},
            row.position,
            pendulum.instance(row.time, 'UTC'),
        )

    @classmethod
    def from_sql_row(cls, row: Any) -> 'Message':
        """Optimized method for returning a new Message instance when the
        row Record is the result from performing a SQL query.
        """
        return cls(
            row.stream_name,
            row.type,
            row.data,
            row.id,
            row.metadata or {},
            row.position,
            pendulum.instance(row.time, 'UTC'),
        )

    def __gt__(self, other):
        if isinstance(other, datetime) and self.time:
            return self.time > other
        elif not hasattr(other, 'position'):
            raise NotImplementedError
        return self.position > other.position

    def __ge__(self, other):
        if isinstance(other, datetime) and self.time:
            return self.time >= other
        elif not hasattr(other, 'position'):
            raise NotImplementedError
        return self.position >= other.position

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            raise NotImplementedError
        return self.stream == other.stream \
            and self.type == other.type \
            and self.data == other.data \
            and self.metadata == other.metadata \
            and self.time == other.time

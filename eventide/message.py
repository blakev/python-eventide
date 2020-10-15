#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from uuid import UUID, uuid4
from functools import total_ordering
from dataclasses import field, asdict, dataclass
from typing import Dict, List, Optional

from eventide._types import JSON


@dataclass(frozen=False, repr=True)
class Metadata:
    """A message's metadata object contains information about the stream where the
    message resides, the previous message in a series of messages that make up a
    messaging workflow, the originating process to which the message belongs, as well
    as other data that are pertinent to understanding the provenance and disposition.

    Message metadata is data about messaging machinery, like message schema version,
    source stream, positions, provenance, reply address, and the like.
    """
    # yapf: disable
    stream_name:                        Optional[str] = field(default=None)
    position:                           Optional[int] = field(default=None)
    global_position:                    Optional[int] = field(default=None)
    causation_message_stream_name:      Optional[str] = field(default=None)
    causation_message_position:         Optional[int] = field(default=None)
    causation_message_global_position:  Optional[int] = field(default=None)
    correlation_stream_name:            Optional[str] = field(default=None)
    reply_stream_name:                  Optional[str] = field(default=None)
    schema_version:                     Optional[str] = field(default=None)
    time:                               Optional[float] = field(default=None)
    # yapf: enable

    @property
    def identifier(self) -> str:
        return '%s/%d' % (self.stream_name, self.position)

    @property
    def causation_identifier(self) -> str:
        return '%s/%d' % (
            self.causation_message_stream_name, self.causation_message_position
        )

    @property
    def replies(self) -> bool:
        return bool(self.reply_stream_name)

    def do_not_reply(self) -> 'Metadata':
        self.reply_stream_name = None
        return self

    def follow(self, other: 'Metadata') -> 'Metadata':
        self.causation_message_stream_name = other.stream_name
        self.causation_message_position = other.position
        self.causation_message_global_position = other.global_position
        self.correlation_stream_name = other.correlation_stream_name
        self.reply_stream_name = other.reply_stream_name
        return self

    def follows(self, other: 'Metadata') -> bool:
        return self.causation_message_stream_name == other.stream_name \
            and self.causation_message_position == other.position \
            and self.causation_message_global_position == other.global_position \
            and self.correlation_stream_name == other.correlation_stream_name \
            and self.reply_stream_name == other.reply_stream_name

    def correlates(self, stream_name: str) -> bool:
        return self.correlation_stream_name == stream_name


@dataclass(frozen=False, repr=False, init=True, eq=False)
class Message:

    id: UUID = field(init=False)
    metadata: Metadata = field(init=False)

    def __post_init__(self):
        self.id = uuid4()
        self.metadata = Metadata()

    def __eq__(self, other: 'Message') -> bool:
        if not isinstance(other, self.__class__):
            return False
        attrs = self.attributes()
        for k, v in other.attributes().items():
            if attrs.get(k, not v) != v:
                return False
        return True

    @property
    def type(self) -> str:
        return self.__class__.__name__

    def attributes(self) -> Dict:
        return asdict(self)

    def attribute_names(self) -> List[str]:
        return list(self.attributes().keys())

    def follow(self, other: 'Message') -> 'Message':
        self.metadata.follow(other.metadata)
        return self

    def follows(self, other: 'Message') -> bool:
        return self.metadata.follows(other.metadata)


@dataclass(frozen=True, repr=True)
@total_ordering
class MessageData:
    """MessageData is the raw, low-level storage representation of a message."""

    type: str
    stream_name: str
    data: JSON = field(default_factory=dict)
    metadata: JSON = field(default_factory=dict)
    id: UUID = field(default_factory=uuid4)
    position: int = field(default=-1)
    global_position: int = field(default=-1)
    time: float = field(default=-1.0)

    def __gt__(self, other: 'MessageData') -> bool:
        return self.global_position > other.global_position

    def __ge__(self, other: 'MessageData') -> bool:
        return self.global_position >= other.global_position

    def __eq__(self, other: 'MessageData') -> bool:
        return self.stream_name == other.stream_name \
            and self.type == other.type \
            and self.data == other.data \
            and self.metadata == other.metadata

    @property
    def category(self) -> str:
        return self.stream_name.split('-')[0]

    @property
    def is_category(self) -> bool:
        return '-' not in self.stream_name

    @property
    def stream_id(self) -> Optional[str]:
        if '-' not in self.stream_name:
            return None
        return self.stream_name.split('-', 1)[1]

    @property
    def cardinal_id(self) -> Optional[str]:
        if '-' not in self.stream_name:
            return None
        return self.stream_name.split('-', 1)[1].split('+')[0]

    @property
    def command(self) -> Optional[str]:
        if ':' not in self.category:
            return None
        return self.category.split(':', 1)[1].split('-')[0]
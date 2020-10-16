#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from uuid import UUID, uuid4
from operator import attrgetter
from functools import total_ordering
from dataclasses import field, asdict, dataclass, fields, make_dataclass, _process_class
from typing import Dict, List, Optional, Callable, NamedTuple, Mapping, Type

from toolz.functoolz import curry

from eventide._types import JSON
from eventide.utils import jdumps, jloads, dense_dict

f_blank = field(default=None, repr=False)


@dataclass(frozen=False, repr=False)
class Metadata:
    """A message's metadata object contains information about the stream where the
    message resides, the previous message in a series of messages that make up a
    messaging workflow, the originating process to which the message belongs, as well
    as other data that are pertinent to understanding the provenance and disposition.

    Message metadata is data about messaging machinery, like message schema version,
    source stream, positions, provenance, reply address, and the like.
    """
    # yapf: disable
    stream_name:                        Optional[str] = f_blank
    position:                           Optional[int] = f_blank
    global_position:                    Optional[int] = f_blank
    causation_message_stream_name:      Optional[str] = f_blank
    causation_message_position:         Optional[int] = f_blank
    causation_message_global_position:  Optional[int] = f_blank
    correlation_stream_name:            Optional[str] = f_blank
    reply_stream_name:                  Optional[str] = f_blank
    schema_version:                     Optional[str] = f_blank
    time:                               Optional[float] = f_blank
    # yapf: enable

    def __repr__(self) -> str:
        # dynamically scan the available fields the first time this
        #  object instance is printed out, looking for fields where
        # repr=True -- we then save those fields so we can dynamically
        #  extract their current value each time.
        attr = '__repr_fields__'
        if not hasattr(self, attr):
            repr_fields = filter(lambda f: f.repr, fields(self))
            repr_fields = set(map(attrgetter('name'), repr_fields))
            setattr(self, attr, repr_fields)
        o = ', '.join('%s=%s' % (k, getattr(self, k)) for k in getattr(self, attr))
        return '%s(%s)' % (self.__class__.__name__, o)

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


@dataclass(frozen=True, repr=True)
@total_ordering
class MessageData:
    """MessageData is the raw, low-level storage representation of a message.

    These instances are READ from the database and should not be created directly.
    """

    type: str
    stream_name: str
    data: JSON
    metadata: JSON
    id: UUID
    position: int
    global_position: int
    time: float

    @classmethod
    def from_record(cls, record: Mapping) -> 'MessageData':
        """Build a new instance from a row in the message store."""
        rec = dict(record)
        rec['data'] = jloads(rec.get('data', '{}'))
        rec['metadata'] = jloads(rec.get('metadata', '{}'))
        return cls(**rec)

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


class SerializedMessage(NamedTuple):
    """A light representation of a Message instance before writing to message store."""
    id: str
    stream_name: str
    type: str
    data: str
    metadata: str
    expected_version: Optional[int]


@dataclass(frozen=False, repr=False, init=True, eq=False)
class Message:
    """Base class for defining custom Message records for the message store.

    Messages are converted into SerializedMessage right before being written, and
    are created from MessageData instances when being deserialized.

    This class should not be instantiated directly but instead should be the parent
    class on other structures that are persisted to the database.
    """

    id: UUID = field(init=False, default_factory=uuid4)
    metadata: Metadata = field(init=False, default_factory=Metadata)

    @classmethod
    def from_messagedata(cls, data: 'MessageData', strict: bool = False) -> 'Message':
        if strict:
            if data.type != cls.__name__:
                raise ValueError('invalid class name, does not match type `%s`' % data.type)
        # coerce the metadata object
        # .. attempt to assign all the metadata fields and values from the
        #  incoming MessageData instance onto this custom Message instance.
        # These additional attributes can be specified before the underlying Message
        #  instance is created by decorating the class with @messagecls.
        meta_obj = {}
        meta_fields = cls.__dataclass_fields__['metadata'].metadata or {}
        for k, v in data.metadata.items():
            if k not in meta_fields:
                if strict:
                    raise ValueError('undefined metadata field name `%s`' % k)
                # else:
                #   skipping field: value
            if k in meta_fields:
                meta_obj[k] = v
        # create instance
        msg = cls(**data.data)
        msg.id = data.id
        msg.metadata = msg.metadata.__class__(**meta_obj)
        # return instance of custom class
        return msg

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

    def serialize(
        self,
        stream_name: str,
        expected_version: Optional[int] = None,
        json_default_fn: Optional[Callable] = None,
    ) -> SerializedMessage:
        """Prepare this instance to be written to the message store.

        Returns a serialized version of this object's data.
        """
        data = self.attributes()
        # separate the metadata from the data
        meta = dense_dict(data.pop('metadata'))
        # remove the UUID, since it has its own column
        del data['id']
        # build the response instance
        return SerializedMessage(
            str(self.id),
            stream_name,
            self.type,
            jdumps(data, json_default_fn),
            jdumps(meta, json_default_fn),
            expected_version,
        )


def messagecls(
    cls_=None,
    *,
    msg_meta: Type[Metadata] = Metadata,
    init=True,
    repr=True,
    eq=True,
    order=False,
    unsafe_hash=False,
    frozen=False,
) -> Type[Message]:
    """Decorator used to build a custom Message type, with the ability to bind
    a custom Metadata class with additional fields. When these instances are built,
    serialized, or de-serialized from the database all the correct fields will be
    filled out with no interference on in-editor linters.

    The parameters for this decorator copy @dataclass with the addition of ``msg_meta``
     which allows the definition to have a custom Metadata class assigned to it.

    All @messagecls decorated classes behave like normal dataclasses.
    """
    def wrap(cls):
        # turn the wrapped class into a dataclass
        kls = dataclass(
            cls,
            init=init,
            repr=repr,
            eq=eq,
            order=order,
            unsafe_hash=unsafe_hash,
            frozen=frozen,
        )
        # extract all the field names and types from the new class definition
        m_fields = {f.name: f.type for f in fields(msg_meta)}
        # re-create the msg_meta class on the `metadata` attribute for this Message
        #  object. We attach the new (and old) fields into the metadata flag for
        # this field so we don't have to process those values every time an instance
        #  is de-serialized from the database.
        return make_dataclass(
            cls.__name__,
            fields=[
                (
                    'metadata',
                    msg_meta,
                    field(
                        init=False,
                        default_factory=msg_meta,
                        metadata=m_fields,
                    ),
                ),
            ],
            bases=(
                kls,
                Message,
            ),
        )
    # ensure this class definition follows basic guidelines
    if not hasattr(msg_meta, '__dataclass_fields__'):
        raise ValueError('custom message metadata class must be a @dataclass')

    if not issubclass(msg_meta, Metadata):
        raise ValueError('custom message metadata class must inherit eventide.Metadata')

    # "wrap" the Metadata class with @dataclass so we don't have to on its definition
    msg_meta = _process_class(msg_meta, True, False, True, False, False, False)

    # mimic @dataclass functionality
    if cls_ is None:
        return wrap
    return wrap(cls_)


message_cls = messagecls  # alias



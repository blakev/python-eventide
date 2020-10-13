#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from hashlib import md5
from uuid import UUID
from typing import Optional, Tuple, AsyncIterable, Dict, Set

from toolz.dicttoolz import keymap

from eventide.types import JSON, Number
from eventide.message import Message
from eventide.database import Database
from eventide.constants import DEFAULT_GEN_UUID, DEFAULT_JSON_DUMPS

__all__ = [
    'MessageStore',
]


# yapf: disable
class Procedures:
    """Known procedure, function and view names for extracting information
    from the PostgreSQL database backend."""

    acquire_lock            = 'acquire_lock'
    get_category_messages   = 'get_category_messages'
    get_last_stream_message = 'get_last_stream_message'
    get_version             = 'message_store_version'
    get_stream_messages     = 'get_stream_messages'
    get_stream_version      = 'stream_version'
    write_message           = 'write_message'
    type_summary            = "SELECT * FROM type_summary;"
    category_type_summary   = "SELECT * FROM category_type_summary;"
    last_message = """
        SELECT * 
        FROM messages
        ORDER BY time DESC
        LIMIT 1;
    """
# yapf: enable


class MessageStore(Database):

    DELIM = '-'
    CARD_DELIM = '+'

    def __init__(self, gen_uuid=None, json_dumps=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gen_uuid = gen_uuid or DEFAULT_GEN_UUID
        self.json_dumps = json_dumps or DEFAULT_JSON_DUMPS

    @classmethod
    def category(cls, stream: str) -> str:
        return stream.split(cls.DELIM)[0]

    @classmethod
    def is_category(cls, stream: str) -> bool:
        return cls.DELIM not in stream

    @classmethod
    def stream_id(cls, stream: str) -> Optional[str]:
        if cls.DELIM not in stream:
            return None
        return stream.split(cls.DELIM, 1)[1]

    @classmethod
    def stream_cardinal_id(cls, stream: str) -> Optional[str]:
        if cls.DELIM not in stream:
            return None
        return stream.split(cls.DELIM, 1)[1].split(cls.CARD_DELIM)[0]

    @classmethod
    def hash64(cls, stream: str) -> int:
        return int(md5(stream.encode('utf-8')).hexdigest()[:16], 16)

    async def acquire_lock(self, stream: str) -> int:
        async with self.cursor() as c:
            await c.callproc(Procedures.acquire_lock, (stream,))
            return (await c.fetchone()).acquire_lock

    async def get_version(self) -> Tuple[int, ...]:
        async with self.cursor() as c:
            await c.callproc(Procedures.get_version, None)
            result = await c.fetchone()
            version = tuple(map(int, result.message_store_version.split('.')))
        return version

    async def get_all_categories(self) -> Set[str]:
        return set([k[0] for k in (await self.category_type_summary()).keys()])

    async def type_summary(self) -> Dict[str, Dict[str, Number]]:
        results = {}
        async with self.cursor() as c:
            await c.execute(Procedures.type_summary, None)
            async for row in c:
                results[row.type] = {
                    'count': row.message_count or 0,
                    'percent': float(row.percent or 0),
                }
        return results

    async def category_type_summary(self) -> Dict[Tuple[str, str], Dict]:
        results = {}
        async with self.cursor() as c:
            await c.execute(Procedures.category_type_summary, None)
            async for row in c:
                results[(
                    row.category,
                    row.type,
                )] = {
                    'count': row.message_count or 0,
                    'percent': float(row.percent or 0),
                }
        return results

    async def type_category_summary(self) -> Dict[Tuple[str, str], Dict]:
        results = keymap(reversed, await self.category_type_summary())
        return {tuple(k): v for k, v in results.items()}

    async def write_message(
        self,
        stream: str,
        type_: str,
        data: Optional[JSON] = None,
        metadata: Optional[JSON] = None,
        id_: Optional[UUID] = None,
        expected_version: Optional[int] = None,
    ) -> Message:
        async with self.cursor() as c:
            id_ = id_ or self.gen_uuid()
            data = data or {}
            await c.callproc(
                Procedures.write_message, (
                    str(id_),
                    stream,
                    type_,
                    self.json_dumps(data),
                    self.json_dumps(metadata) if metadata else None,
                    expected_version,
                )
            )
            position = (await c.fetchone()).write_message or -1
        return Message(stream, type_, data, id_, metadata, position)

    async def get_category_messages(
        self,
        category: str,
        position: int = 1,
        batch_size: int = 1000,
        correlation: Optional[str] = None,
        consumer_group_member: Optional[int] = None,
        consumer_group_size: Optional[int] = None,
        sql_condition: Optional[str] = None,
    ) -> AsyncIterable[Message]:
        async with self.cursor() as c:
            await c.callproc(
                Procedures.get_category_messages, (
                    category,
                    max(0, position),
                    max(1, batch_size),
                    correlation,
                    consumer_group_member,
                    consumer_group_size,
                    sql_condition,
                )
            )
            async for row in c:
                yield Message.from_row(row)

    async def get_stream_messages(
        self,
        stream: str,
        position: int = 0,
        batch_size: int = 1000,
        sql_condition: Optional[str] = None,
    ) -> AsyncIterable[Message]:
        """Get messages from a stream.

        Retrieve messages from a single stream, optionally specifying the starting
        position, the number of messages to retrieve, and an additional condition
        that will be appended to the SQL command's WHERE clause.
        """
        async with self.cursor() as c:
            await c.callproc(
                Procedures.get_stream_messages, (
                    stream,
                    max(0, position),
                    max(1, batch_size),
                    sql_condition,
                )
            )
            async for row in c:
                yield Message.from_row(row)

    async def get_last_stream_message(self, stream: str) -> Optional[Message]:
        async with self.cursor() as c:
            await c.callproc(Procedures.get_last_stream_message, (stream,))
            result = await c.fetchone()
            if result:
                return Message.from_row(result)

    async def get_stream_version(self, stream: str) -> Optional[int]:
        async with self.cursor() as c:
            await c.callproc(Procedures.get_stream_version, (stream,))
            return (await c.fetchone()).stream_version

    async def get_last_message(self) -> Optional[Message]:
        async with self.cursor() as c:
            await c.execute(Procedures.last_message)
            result = await c.fetchone()
            if result:
                return Message.from_sql_row(result)

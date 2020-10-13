#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from uuid import UUID
from typing import Optional, Tuple, AsyncIterable

from psycopg2 import DatabaseError

from eventide.types import JSON
from eventide.message import Message
from eventide.database import Database
from eventide.constants import DEFAULT_GEN_UUID, DEFAULT_JSON_DUMPS

__all__ = [
    'MessageStore',
]


class Procedures:
    get_version = 'message_store_version'
    write_message = 'write_message'


class MessageStore(Database):

    gen_uuid = DEFAULT_GEN_UUID
    json_dumps = DEFAULT_JSON_DUMPS

    async def is_category(
        self,
        stream: str,
    ) -> bool:
        pass

    async def write_message(
        self,
        stream: str,
        type_: str,
        data: Optional[JSON] = None,
        metadata: Optional[JSON] = None,
        id_: Optional[UUID] = None,
        expected_version: Optional[int] = None,
    ) -> Message:
        async with self.cursor() as cur:
            id_ = id_ or self.gen_uuid()
            data = data or {}
            await cur.callproc(
                Procedures.write_message, (
                    str(id_),
                    stream,
                    type_,
                    self.json_dumps(data),
                    self.json_dumps(metadata) if metadata else None,
                    expected_version,
                )
            )
            position = (await cur.fetchone()).write_message or -1
        return Message(stream, type_, data, id_, metadata, position)

    async def get_last_message(self) -> Optional[Message]:
        pass

    async def get_last_stream_message(
        self,
        stream: str,
    ) -> Optional[Message]:
        pass

    async def get_stream_messages(
        self,
        stream: str,
        position: int = 0,
        batch_size: int = 1000,
        sql_condition: Optional[str] = None,
    ) -> AsyncIterable[Message]:
        pass

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
        pass

    async def get_stream_version(
        self,
        stream: str,
    ) -> int:
        pass

    async def get_version(self) -> Optional[Tuple[int, ...]]:
        try:
            async with self.cursor() as cur:
                await cur.callproc(Procedures.get_version, None)
                result = await cur.fetchone()
        except DatabaseError as e:
            self.logger.error(e)
            version = None
        else:
            version = tuple(map(int, result.message_store_version.split('.')))
        return version

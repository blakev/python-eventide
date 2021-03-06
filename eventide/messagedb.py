#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

import math
import asyncio
from asyncio import Queue
from hashlib import md5
from logging import getLogger
from operator import attrgetter
from contextlib import asynccontextmanager
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Callable,
    Optional,
    AsyncIterable,
)

import asyncpg
from asyncpg.pool import Pool
from cytoolz.functoolz import curry
from cytoolz.itertoolz import groupby, partition_all
from asyncpg.connection import Connection
from asyncpg.exceptions import RaiseError, PostgresError

from eventide.utils import jdumps, jloads
from eventide._types import JSONFlatTypes, Loop
from eventide.errors import EventideError
from eventide.message import Message, MessageData, SerializedMessage


class MessageDBError(EventideError):
    """Base exception thrown for errors that occur in the MessageDB instance."""


# yapf: disable
class Procs:
    """Known procedure, function and view names for extracting information
    from the PostgreSQL database backend."""

    hash_64                 = 'SELECT hash_64($1);'
    acquire_lock            = 'SELECT acquire_lock($1);'
    write_message           = 'SELECT write_message($1, $2, $3, $4, $5, $6);'
    get_stream_version      = 'SELECT stream_version($1);'
    get_stream_messages     = 'SELECT get_stream_messages($1, $2, $3, $4);'
    get_last_stream_message = 'SELECT get_last_stream_message($1);'
    get_category_messages   = 'SELECT get_category_messages($1, $2, $3, $4, $5, $6, $7)'
    get_version             = "SELECT message_store_version();"
    sql_last_message = """
        SELECT * 
        FROM messages
        ORDER BY time DESC
        LIMIT 1;
    """
# yapf: enable


class MessageDB:

    DEFAULT_DSN = 'postgresql://message_store@0.0.0.0/message_store'

    def __init__(
        self,
        config: Dict[str, Any],
        max_pending: int = 128,
        json_default_fn: Optional[Callable[[Any], JSONFlatTypes]] = None,
        loop: Loop = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.logger = getLogger('eventide.MessageDB')

        self._config = config
        self._pool: Optional[Pool] = None
        self._pending: Queue = Queue(maxsize=max_pending, loop=self.loop)
        self._jdumps = curry(jdumps)(default=json_default_fn)

    def __repr__(self) -> str:
        return 'MessageDB(connected=%s)' % self.connected

    @property
    def connected(self) -> bool:
        return self._pool and not self._pool._closed

    @asynccontextmanager
    async def connection(self, action: Optional[str] = None) -> Connection:
        """Returns an active Connection to the MessageDB database."""

        async with self._pool.acquire() as con:
            try:
                if action:
                    self.logger.debug('connection action = %s', action)
                yield con
            except RaiseError as e:
                raise MessageDBError(e) from None
            except PostgresError as e:
                raise e
            except Exception as e:
                self.logger.exception(e)
                raise MessageDBError(*e.args) from e

    async def setup(self):
        """Setup must be called before interacting with the message store.
        This method is responsible for setting up the database connections and
        other misc. background tasks to ensure a performant experience."""

        if self._pool is None:
            config = dict(self._config)
            dsn = config.pop('dsn', self.DEFAULT_DSN)
            self._pool = await asyncpg.create_pool(dsn, **config, loop=self.loop)

        # setup json serialization
        async with self.connection('set json') as conn:
            await conn.set_type_codec(
                'json',
                encoder=self._jdumps,
                decoder=jloads,
                schema='pg_catalog',
            )

    async def shutdown(self):
        """Shutdown will terminate all open connections and perform other cleanup
        tasks before delegating control back to the calling application."""

        if self.connected:
            await self._pool.close()

    # ~~~

    @classmethod
    def hash64(cls, value: str) -> int:
        """Computes the 64-bit MD5SUM hash of a value locally."""
        return int(md5(value.encode('utf-8')).hexdigest()[:16], 16)

    async def get_hash64(self, value: str) -> int:
        """Computes the 64-bit MD5SUM hash of a value on the database."""
        async with self.connection('get_hash64') as con:
            return (await con.fetchrow(Procs.hash_64, value))[0]

    async def acquire_lock(self, stream: str) -> int:
        async with self.connection('acquire_lock') as con:
            return (await con.fetchrow(Procs.acquire_lock, stream))[0]

    async def write_message(
        self,
        stream_name: str,
        message: Message,
        expected_version: Optional[int] = None,
    ) -> int:
        """Write a generic message to the database."""
        args = message.serialize(stream_name, expected_version)
        async with self.connection('write_message') as conn:
            return (await conn.fetchrow(Procs.write_message, *args))[0]

    async def queue_message(
        self,
        stream_name: str,
        message: Message,
    ) -> None:
        await self._pending.put(message.serialize(stream_name))

    async def write_pending_messages(self) -> int:
        """Flushes the buffer, if there are items in it, to the message store.

        The return value is the number of records that were successfully synced.
        """
        if self._pending.empty():
            return 0

        total = 0
        split_n = math.ceil(self._pending.maxsize / 4.0)

        # ensure the queue is empty before returning
        while not self._pending.empty():
            pending: List[SerializedMessage] = []

            # flush the entire queue
            while not self._pending.empty():
                pending.append(await self._pending.get())

            # divide all the pending into like-typed instances
            partitioned = groupby(attrgetter('type'), pending)
            for _, items in partitioned.items():
                for idx, bundle in partition_all(split_n, items):
                    async with self.connection('write_pending_messages-%d' % idx) as c:
                        async with c.transaction():
                            for msg in bundle:
                                await c.fetchrow(Procs.write_message, *msg)
                                total += 1
        # ~~ no more in pending queue
        return total

    async def get_version(self) -> Tuple[int, ...]:
        async with self.connection('get_version') as conn:
            res = await conn.fetchrow(Procs.get_version)
            if not res:
                return 0, 0
            return tuple(map(int, res[0].split('.')))

    async def get_last_global_index(self) -> int:
        res = await self.get_last_message()
        return res['global_position'] if res else 0

    async def get_last_message(self) -> Optional[MessageData]:
        async with self.connection('get_last_message') as conn:
            res = await conn.fetchrow(Procs.sql_last_message)
            return MessageData.from_record(res)

    async def get_stream_version(
        self,
        stream: str,
    ) -> int:
        """Gets the stream version by returning its max position."""
        async with self.connection('get_stream_version') as con:
            return (await con.fetchrow(Procs.get_stream_version, stream))[0]

    async def get_stream_messages(
        self,
        stream: str,
        position: int = 0,
        batch_size: int = 1000,
        sql_condition: Optional[str] = None,
    ) -> AsyncIterable[MessageData]:
        """Get messages from a stream.

        Retrieve messages from a single stream, optionally specifying the starting
        position, the number of messages to retrieve, and an additional condition
        that will be appended to the SQL command's WHERE clause."""
        args = (
            stream,
            max(0, position),
            max(1, batch_size),
            sql_condition,
        )
        async with self.connection('get_stream_messages') as con:
            async with con.transaction():
                async for res in con.cursor(Procs.get_stream_messages, *args):
                    yield MessageData.from_record(res)

    async def get_last_stream_message(
        self,
        stream: str,
    ) -> Optional[MessageData]:
        """Get the last message from a stream."""
        async with self.connection('get_stream_last_message') as con:
            res = await con.fetchrow(Procs.get_last_stream_message, stream)
            if not res or len(res) == 0:
                return None
            return MessageData.from_record(res[0])

    async def get_category_messages(
        self,
        category: str,
        position: int = 1,
        batch_size: int = 1000,
        correlation: Optional[str] = None,
        consumer_group_member: Optional[int] = None,
        consumer_group_size: Optional[int] = None,
        sql_condition: Optional[str] = None,
    ) -> AsyncIterable[MessageData]:
        """Get messages from a category."""
        args = (
            category,
            max(0, position),
            max(1, batch_size),
            correlation,
            consumer_group_member,
            consumer_group_size,
            sql_condition,
        )
        async with self.connection('get_category_messages') as con:
            async with con.transaction():
                async for res in con.cursor(Procs.get_category_messages, *args):
                    yield MessageData.from_record(res)

#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

import asyncio
from logging import getLogger
from contextlib import asynccontextmanager
from typing import Optional

from psycopg2 import DatabaseError
from aiopg.pool import Pool
from psycopg2.extras import NamedTupleCursor
from aiopg.connection import TIMEOUT
from psycopg2.extensions import cursor as Cursor

from eventide.types import Loop

__all__ = [
    'Database',
]


class Database:

    def __init__(
        self,
        dsn: str,
        minsize: int = 1,
        maxsize: int = 5,
        timeout: float = TIMEOUT,
        echo: bool = False,
        loop: Loop = None,
    ):
        self._dsn = dsn
        self._min_conn = max(1, minsize)
        self._max_conn = max(1, maxsize)
        self._timeout = max(0.0, timeout)
        self._echo = bool(echo)
        self.__pool: Optional[Pool] = None

        self.loop = loop or asyncio.get_event_loop()
        self.logger = getLogger(f'{__name__}.Database')

    def __del__(self):
        # cleanup all the dangling/open connections
        if self.__pool and self.__pool.freesize > 0:
            self.__pool.terminate()

    @classmethod
    def from_kwargs(
        cls,
        host: str,
        user: str,
        password: str,
        port: str = '5432',
        dbname: str = 'message_store',
        **kwargs,
    ) -> 'Database':
        """Create a new Database instance from keywords, they're then assembled
        into a valid DSN and passed to the init method. Extra kwargs are passed
        unmodified.
        """
        dsn = {
            'host': host,
            'user': user,
            'password': password or '',
            'port': port,
            'dbname': dbname,
        }
        dsn_str = ' '.join(f'{k}={v}' for k, v in dsn.items())
        return cls(dsn=dsn_str, **kwargs)

    @property
    def connected(self) -> bool:
        try:
            connected = self.__pool and not self.__pool.closed
        except Exception:
            connected = False
        return connected

    @property
    def pool(self) -> Optional[Pool]:
        """Attempt to connect to a database using connection pooling."""
        if self.__pool is None:
            try:
                pool = Pool(
                    dsn=self._dsn,
                    minsize=self._min_conn,
                    maxsize=self._max_conn,
                    timeout=self._timeout,
                    echo=self._echo,
                    enable_json=True,
                    enable_uuid=True,
                    enable_hstore=True,
                    pool_recycle=-1,
                    on_connect=None,
                    loop=self.loop,
                )
            except Exception as e:
                self.logger.exception(e)
                return None
            else:
                self.__pool = pool
        return self.__pool

    @asynccontextmanager
    async def cursor(self, cursor_factory: Optional[Cursor] = None):
        if cursor_factory is None:
            cursor_factory = NamedTupleCursor
        async with self.pool.acquire() as conn:
            async with conn.cursor(cursor_factory=cursor_factory) as c:
                try:
                    yield c
                except DatabaseError as e:
                    self.logger.error(e)

    async def close(self) -> None:
        if self.connected:
            self.pool.close()
            await self.pool.clear()
            await self.pool.wait_closed()

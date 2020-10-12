#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

import asyncio
from uuid import UUID
from logging import getLogger
from typing import (
    Any,
    Tuple,
    Callable,
    Optional,
)

from psycopg2 import DatabaseError
from omegaconf import DictConfig

from eventide.types import JSON, Loop
from eventide.message import Message
from eventide.database import Database
from eventide.constants import DEFAULT_GEN_UUID, DEFAULT_JSON_DUMPS


class Eventide:

    def __init__(
        self,
        database: Optional[Database] = None,
        config: Optional[DictConfig] = None,
        fn_gen_uuid: Callable[[], UUID] = DEFAULT_GEN_UUID,
        fn_json_dumps: Callable[[Any], str] = DEFAULT_JSON_DUMPS,
        loop: Loop = None,
    ):
        self._db = database
        self._config = config
        self._jdumps = fn_json_dumps
        self._gen_uuid = fn_gen_uuid
        self.loop = loop or asyncio.get_event_loop()
        self.logger = getLogger(f'{__name__}.Eventide')

    @property
    def database(self) -> Database:
        if self._db is None:
            self._db = Database(**self._config.database or {})
        return self._db

    db = database  # alias

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def write_raw_message(
        self,
        stream: str,
        type_: str,
        data: Optional[JSON] = None,
        metadata: Optional[JSON] = None,
        id_: Optional[UUID] = None,
        expected_version: Optional[int] = None,
    ) -> Message:
        """Write a message to the event store."""
        async with self.db.cursor() as cur:
            id_ = id_ or self._gen_uuid()
            data = data or {}
            await cur.callproc(
                'write_message', (
                    str(id_),
                    stream,
                    type_,
                    self._jdumps(data),
                    self._jdumps(metadata) if metadata else None,
                    expected_version,
                )
            )
            position = (await cur.fetchone()).write_message or -1
        return Message(stream, type_, data, id_, metadata, position)

    async def get_version(self) -> Optional[Tuple[int, ...]]:
        """Executes ``message_store_version`` stored procedure."""
        try:
            async with self.db.cursor() as cur:
                await cur.callproc('message_store_version', None)
                result = await cur.fetchone()
        except DatabaseError as e:
            self.logger.error(e)
            version = None
        else:
            version = tuple(map(int, result.message_store_version.split('.')))
        return version

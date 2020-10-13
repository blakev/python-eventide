#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

import asyncio
from logging import getLogger
from typing import Optional

from omegaconf import DictConfig

from eventide.types import Loop
from eventide.message import Message
from eventide.message_store import MessageStore


class Eventide:

    def __init__(
        self,
        database: Optional[MessageStore] = None,
        config: Optional[DictConfig] = None,
        loop: Loop = None,
    ):
        self._db = database
        self._config = config
        self.loop = loop or asyncio.get_event_loop()
        self.logger = getLogger(f'{__name__}.Eventide')

    @property
    def database(self) -> MessageStore:
        if self._db is None:
            self._db = MessageStore(**self._config.database or {})
        return self._db

    db = mdb = message_store = database  # aliases

    async def close(self) -> None:
        if self._db:
            await self._db.close()


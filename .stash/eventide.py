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
from eventide.message_store import MessageStore
from eventide.handler import Handler


class Eventide:

    def __init__(
        self,
        message_store: Optional[MessageStore] = None,
        config: Optional[DictConfig] = None,
        loop: Loop = None,
    ):
        self._mdb = message_store
        self._config = config
        self.loop = loop or asyncio.get_event_loop()
        self.logger = getLogger(f'{__name__}.Eventide')

    @property
    def message_store(self) -> MessageStore:
        if self._mdb is None:
            self._mdb = MessageStore(**self._config.message_store or {})
        return self._mdb

    mdb = message_store

    async def close(self) -> None:
        if self._mdb:
            await self._mdb.close()

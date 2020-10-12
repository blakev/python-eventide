#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from eventide.database import Database


class Eventide:

    __slots__ = ('_config', '_db')

    def __init__(self, config):
        self._config = config
        self._db = None

    @property
    def database(self) -> Database:
        return self._db

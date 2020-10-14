#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<


class Handler:
    """
    A handler is the entry point of a message into the business logic of a service.
    It receives instructions from other services, apps, and clients in the form of
    commands and events.
    """

    def __init__(self, name: str):
        self._name = name

#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from asyncio import AbstractEventLoop
from typing import (
    Dict,
    List,
    Union,
    Optional,
)

Number = Union[int, float]
JSONFlatTypes = Union[str, int, float, bool, None]
JSONTypes = Union[JSONFlatTypes, List[JSONFlatTypes], Dict[str, JSONFlatTypes]]
JSON = Dict[str, JSONTypes]
Loop = Optional[AbstractEventLoop]

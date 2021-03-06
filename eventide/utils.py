#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from typing import (
    Any,
    Dict,
    Callable,
    Optional, Tuple,
)

import orjson

__all__ = [
    'jdumps',
    'jloads',
    'dense_dict',
]

# yapf: disable
ORJSON_OPTIONS = (
    orjson.OPT_SORT_KEYS |
    orjson.OPT_NAIVE_UTC |
    orjson.OPT_PASSTHROUGH_DATACLASS |
    orjson.OPT_UTC_Z
)
# yapf: enable


def jdumps(value: Any, default: Optional[Callable] = None) -> str:
    return orjson.dumps(value, default=default, option=ORJSON_OPTIONS).decode()


def jloads(value: str) -> Dict:
    return orjson.loads(value)


def dense_dict(dictionary: Dict, removables: Tuple = (None,)) -> Dict:
    """Return a dictionary ignoring keys with None values, recursively."""
    return {
        k: dense_dict(v) if isinstance(v, dict) else v
        for k, v in dictionary.items()
        if v not in removables
    }

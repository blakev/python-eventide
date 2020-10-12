#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from invoke import Collection

from tasks import build

ns = Collection(build=build,)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from .run import main
except ImportError:
    from .stub import main  # noqa

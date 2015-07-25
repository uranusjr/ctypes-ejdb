#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys


def main(*args, **kwargs):
    print(
        'Command line interface not installed. To get it, run this:\n\n'
        '\tpip install ctypes-ejdb[cli]\n',
        file=sys.stderr,
    )
    sys.exit(1)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import platform

import pytest


def skipifpypy(reason):
    return pytest.mark.skipif(
        platform.python_implementation() == 'PyPy',
        reason=reason,
    )

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ejdb import utils

from .utils import skipifpypy


class TestCObjectWrapper(object):

    @skipifpypy(reason='PyPy uses garbage collection.')
    def test_del_finalizer_ran(self):

        def finalizer(wrapped):
            finalizer._ran = True

        finalizer._ran = False

        wrapper = utils.CObjectWrapper(object(), finalizer)
        assert finalizer._ran is False
        del wrapper
        assert finalizer._ran is True

    @skipifpypy(reason='PyPy uses garbage collection.')
    def test_scope_finalizer_ran(self):

        def finalizer(wrapped):
            finalizer._ran = True

        finalizer._ran = False

        # Create an inner scope so that `wrapper` inside is garbage collected.
        def inner():
            wrapper = utils.CObjectWrapper(object(), finalizer)     # noqa

        assert finalizer._ran is False
        inner()
        assert finalizer._ran is True

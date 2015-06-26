#!/usr/bin/env python
# -*- coding: utf-8 -*-

import weakref

import six


# Keeps a reference to all wrapper instances so that we can dealloc them when
# we need to.
_tracked_refs = {}


def _run_finalizer(ref):
    """Internal weakref callback to run finalizers.
    """
    del _tracked_refs[id(ref)]
    ref.finalizer(ref.wrapped)


# Must subclass `weakref.ref` so that we can add attributes to it.
class _Ref(weakref.ref):
    pass


class CObjectWrapper(object):
    """C object wrapper using a weakref for memory management.

    Based on `http://code.activestate.com/recipes/577242`.

    :param wrapped: A C object to be wrapped. This will be accessible with
        `self._wrapped`.
    :param finalizer: A callable that takes one argument `wrapped`.
    """
    def __init__(self, wrapped, finalizer):
        self._wrapped = wrapped
        ref = _Ref(self, _run_finalizer)
        ref.wrapped = wrapped
        ref.finalizer = finalizer
        _tracked_refs[id(ref)] = ref


def python_2_unicode_compatible(klass):
    """A decorator that defines __unicode__ and __str__ methods under Python 2.
    Under Python 3 it does nothing.

    To support Python 2 and 3 with a single code base, define a __str__ method
    returning text and apply this decorator to the class.

    Taken from Django (in `django.utils.encoding`).
    """
    if six.PY2:
        if '__str__' not in klass.__dict__:
            raise ValueError("@python_2_unicode_compatible cannot be applied "
                             "to %s because it doesn't define __str__()." %
                             klass.__name__)
        klass.__unicode__ = klass.__str__
        klass.__str__ = lambda self: self.__unicode__().encode('utf-8')
    return klass


ENCODING = 'utf-8'     # TODO: Use system encoding.


def coerce_char_p(s):
    if isinstance(s, six.text_type):
        s = s.encode(ENCODING)
    return s


def coerce_str(s):
    if isinstance(s, six.binary_type):
        return s.decode(ENCODING)
    return six.text_type(s)

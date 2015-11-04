#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import os
import weakref

import six

try:    # pragma: no cover
    from configparser import ConfigParser, NoSectionError, NoOptionError
except ImportError:     # Python 2.     # pragma: no cover
    from ConfigParser import (
        SafeConfigParser as ConfigParser, NoSectionError, NoOptionError
    )


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


if six.PY3:     # pragma: no cover
    recursive_repr = six.moves.reprlib.recursive_repr
else:           # pragma: no cover
    from thread import get_ident

    def recursive_repr(fillvalue='...'):
        """Decorator to make a repr function return fillvalue for a recursive
        call.

        Copied from Python 3 stdlib's `Lib/reprlib.py`.
        """

        def decorating_function(func):
            repr_running = set()

            def wrapper(self):
                key = id(self), get_ident()
                if key in repr_running:
                    return fillvalue
                repr_running.add(key)
                try:
                    result = func(self)
                finally:
                    repr_running.discard(key)
                return result

            # Can't use functools.wraps() here because of bootstrap issues
            wrapper.__module__ = getattr(func, '__module__')
            wrapper.__doc__ = getattr(func, '__doc__')
            wrapper.__name__ = getattr(func, '__name__')
            wrapper.__annotations__ = getattr(func, '__annotations__', {})
            return wrapper

        return decorating_function


class PrettyOrderedDict(collections.OrderedDict):
    """OrderedDict functionality with dict appearance.
    """
    @recursive_repr()
    def __repr__(self):
        return dict.__repr__(self)


def get_package_root():
    cur = os.path.abspath(__file__)
    while True:
        name = os.path.basename(cur)
        if name == 'ejdb':
            return cur
        elif not name:
            return ''
        cur = os.path.dirname(cur)


def read_ejdb_config():
    root = get_package_root()
    if not root:
        return None
    config_path = os.path.join(root, 'ejdb.cfg')
    if not os.path.exists(config_path):
        return None
    parser = ConfigParser()
    parser.read([os.path.expanduser('~/.ejdb.cfg'), config_path])
    try:
        return parser.get('ejdb', 'path')
    except (NoSectionError, NoOptionError):
        return None


def python_2_unicode_compatible(klass):     # pragma: no cover
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

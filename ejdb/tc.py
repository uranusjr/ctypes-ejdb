#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numbers

from . import c
from .utils import CObjectWrapper


class ListIterator(CObjectWrapper):
    """Python iterator wrapper for a `TCLIST *`.
    """
    def __init__(self, wrapped, count=None):
        super(ListIterator, self).__init__(
            wrapped=wrapped, finalizer=c.tc.listdel,
        )
        if count is None:
            count = c.tc.listnum(wrapped)
        self._len = count
        self._i = 0

    def __iter__(self):     # pragma: no cover
        return self

    def __len__(self):
        return self._len

    def __getitem__(self, key):
        if isinstance(key, slice):
            return [self[i] for i in range(*key.indices(len(self)))]
        elif isinstance(key, numbers.Number):
            if key >= len(self):
                raise IndexError('Iterator index out of range.')
            value_p = c.tc.listval2(self._wrapped, key)
            return self.instantiate(value_p)
        return NotImplemented

    def __next__(self):
        if self._i >= self._len:
            raise StopIteration
        value_p = c.tc.listval2(self._wrapped, self._i)
        self._i += 1
        return self.instantiate(value_p)

    def next(self):     # pragma: no cover
        """Python 2 compatibility.
        """
        return self.__next__()

    def instantiate(self, value_p):
        """Subclasses should override this method to instantiate an item during
        iteration.

        :param value_p: Points to the current TCList iterator value of type
            `c_void_p`.
        """
        raise NotImplementedError

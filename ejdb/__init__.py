#!/usr/bin/env python
# -*- coding: utf-8 -*-

VERSION = (0, 4, 0,)

__author__ = 'Tzu-ping Chung'
__email__ = 'uranusjr@gmail.com'
__version__ = '.'.join(str(v) for v in VERSION)


from .api import (      # noqa
    CollectionDoesNotExist, DatabaseError, TransactionError, OperationError,
    READ, WRITE, CREATE, TRUNCATE, NOLOCK, SYNC,
    STRING, ISTRING, NUMBER, ARRAY,
    get_ejdb_version, is_valid_oid, Collection, Database,
)
from .c import init     # noqa

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import collections
import ctypes
import functools

import six

from . import bson, c, tc
from .utils import CObjectWrapper, coerce_char_p, coerce_str


class DatabaseError(Exception):
    pass


class CollectionDoesNotExist(KeyError, DatabaseError):
    pass


class CommandError(ValueError, DatabaseError):
    pass


class TransactionError(DatabaseError):
    pass


class OperationError(DatabaseError):
    pass


Index = collections.namedtuple('Index', ['flags', 'name'])


READ = c.JBOREADER
"""Open as a reader."""

WRITE = c.JBOWRITER
"""Open as a writer."""

CREATE = c.JBOCREAT
"""Create db if it not exists."""

TRUNCATE = c.JBOTRUNC
"""Truncate db."""

NOLOCK = c.JBONOLCK
"""Open without locking."""

NOBLOCK = c.JBOLCKNB
"""Lock without blocking."""

SYNC = c.JBOTSYNC
"""Synchronize every transaction."""

STRING = Index(c.JBIDXSTR, 'string')
"""A string index type."""

ISTRING = Index(c.JBIDXISTR, 'case-insensitive string')
"""A string index type, case insensitive."""

NUMBER = Index(c.JBIDXNUM, 'number')
"""A number index type."""

ARRAY = Index(c.JBIDXARR, 'array')
"""An array index type."""


def _ejdb_finalizer(wrapped):
    if c.ejdb.isopen(wrapped):
        c.ejdb.close(wrapped)
    c.ejdb.del_(wrapped)


def _set_index(collection, verb, path, index_type, flags=0):
    path_c = coerce_char_p(path)
    flags |= index_type.flags
    ok = c.ejdb.setindex(collection._wrapped, path_c, flags)
    if not ok:
        raise DatabaseError(
            'Could not {verb} {index_type_name} index '
            'on collection {collname} with path {path}.'.format(
                verb=verb, index_type_name=index_type.name,
                collname=collection.name, path=path,
            )
        )


def _get_errmsg(db):
    if isinstance(db, CObjectWrapper):
        db = db._wrapped
    errcod = c.ejdb.ecode(db)
    errmsg = c.ejdb.errmsg(errcod)
    msg = coerce_str(errmsg)
    return msg[0].upper() + msg[1:] + '.'


def _get_id(document):
    return document.get(c.JDBIDKEYNAME, document[bson.ID_KEY_NAME])


def _init_c(func):
    """Decorator that initialize the C bindings if needed.
    """
    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        if not c.initialized:
            c.init()
        return func(*args, **kwargs)

    return _decorated


@_init_c
def get_ejdb_version():
    """Get version of the underlying EJDB C library.
    """
    return coerce_str(c.ejdb.version())


@_init_c
def is_valid_oid(s):
    """Check whether the given string can be used as an OID in EJDB.

    The current OID format (as of 1.2.x) is a 24-character-long hex string.
    """
    s = coerce_char_p(s)
    validness = c.ejdb.isvalidoidstr(s)
    return validness


class Cursor(tc.ListIterator):
    """Cursor to iterate through the document result set.

    Instances of this class are returned by a retrieval method, e.g.
    :func:`Collection.find`. You generally should not instantiate a cursor
    directly.
    """
    def __eq__(self, other):
        if self is other:
            return True
        if len(self) != len(other):
            return False
        return all(x == y for x, y in six.moves.zip(self, other))

    def __ne__(self, other):
        return not (self == other)

    def instantiate(self, value_p):
        # `value_p` from `TCLIST *` is already managed, and we don't want the
        # BSON class to manage it, only the rest of the bson struct. The
        # solution is to pretend the data is on the stack (i.e. immutable),
        # and `bson_del` would do the right thing (not freeing things on the
        # stack). Since this is not really a stack value, we don't care about
        # `mincapacity` (the third argument), so we pass 0. `maxonstack` (the
        # fourth argument) is the bson data's length.
        wrapped = c.bson.create()
        c.bson.init_on_stack(wrapped, value_p, 0, c.bson.size2(value_p))
        bs = bson.BSON(wrapped)
        obj = bs.decode()
        return obj


class Transaction(object):

    def __init__(self, collection, allow_nested):
        super(Transaction, self).__init__()
        self._collection = collection

        # Begin transaction immediately, so that direct call on
        # `Collection.begin_transaction()` works without a `with` block.
        if allow_nested and collection.is_in_transaction():
            self._should_exit = False
        else:
            self._should_exit = True
            ok = c.ejdb.tranbegin(collection._wrapped)
            if not ok:
                raise TransactionError('Could not begin transaction.')

    def __enter__(self):
        # Don't need to do anything since the transaction has already begun.
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if self._should_exit:
            if exc_type:
                self._collection.abort_transaction()
                return False    # Do not swallow the exception.
            self._collection.commit_transaction()


class Collection(object):
    """Representation of a collection inside a database.

    You generally should not instantiate a collection directly. Call
    :func:`Database.get_collection` to get a collection inside a database
    instead.
    """
    def __init__(self, database, wrapped):
        super(Collection, self).__init__()
        self._database = database
        self._wrapped = wrapped

    def __repr__(self):
        return '<Collection {name}>'.format(name=self.name)

    @property
    def database(self):
        """The :class:`Database` instance this collection belongs to.
        """
        return self._database

    @property
    def name(self):
        """Name of this collection.
        """
        wrapped_v = self._wrapped.contents
        return coerce_str(wrapped_v.cname)

    def drop(self):
        self.database.drop_collection(self.name)

    def is_in_transaction(self):
        in_tran = ctypes.c_bool()
        c.ejdb.transtatus(self._wrapped, ctypes.byref(in_tran))
        return in_tran.value

    def begin_transaction(self, allow_nested=False):
        """Begin a transaction on this collection.

        This can be used directly, with the user calling
        :func:`commit_transaction` or :func:`abort_transaction` later
        manually::

            collection.begin_transaction()
            try:
                ... # Do things.
            except:
                collection.abort_transaction()
                raise
            else:
                collection.commit_transaction()

        Or as a context manager::

            with collection.begin_transaction():
                ... # Do things.

        In the latter usage, :func:`abort_transaction` will be called
        automatically when the block exits with an exception; if the block
        exits normally, :func:`commit_transaction` will be called.
        """
        if not allow_nested and self.is_in_transaction():
            raise TransactionError('Already in a transaction.')
        return Transaction(collection=self, allow_nested=allow_nested)

    def commit_transaction(self):
        """Commit a transaction.
        """
        if not self.is_in_transaction():
            raise TransactionError('Not in a transaction.')
        ok = c.ejdb.trancommit(self._wrapped)
        if not ok:
            raise TransactionError('Could not commit transaction.')

    def abort_transaction(self):
        """Abort a transaction, discarding all un-committed operations.
        """
        if not self.is_in_transaction():
            raise TransactionError('Not in a transaction.')
        ok = c.ejdb.tranabort(self._wrapped)
        if not ok:
            raise TransactionError('Could not abort transaction.')

    def _perform_save(self, document, merge):
        bs = bson.encode(document)
        oid = c.BSONOID()
        ok = c.ejdb.savebson2(
            self._wrapped, bs._wrapped, ctypes.byref(oid), merge,
        )
        if not ok:
            raise DatabaseError(_get_errmsg(self.database))
        return oid

    def _insert(self, document):
        try:
            doc_id = _get_id(document)
        except KeyError:
            pass
        else:
            oid = c.BSONOID.from_string(doc_id)
            bsdata = c.ejdb.loadbson(self._wrapped, ctypes.byref(oid))
            if bsdata:  # Matching OID exists.
                raise OperationError(
                    'Could not insert document. Matching OID exists.'
                )
        oid = self._perform_save(document, merge=False)
        return oid

    def insert_one(self, document):
        """Insert a single document.

        :returns: OID of the inserted document.
        """
        oid = self._insert(document)
        return six.text_type(oid)

    def insert_many(self, documents):
        """Insert a list of documents.

        :returns: A list of OIDs of the inserted documents.
        """
        with self.begin_transaction():
            ids = [
                six.text_type(self._insert(document))
                for document in documents
            ]
        return ids

    def _execute(self, queries, hints, flags, query_items=None):
        query = query_items or {}
        if queries:
            query.update({k: v for k, v in queries[0].items()})
            queries = queries[1:]
        query_bs = bson.encode(query, as_query=True)
        hints_bs = bson.encode(hints, as_query=True)

        extra_query_count = len(queries)
        if extra_query_count:
            BSONREF_ARR = c.BSONREF * extra_query_count
            extra_query_bs_array = BSONREF_ARR(*(
                bson.encode(obj, as_query=True)._wrapped for obj in queries
            ))
        else:
            extra_query_bs_array = c.BSONREF(0)

        ejq = c.ejdb.createquery(
            self._database._wrapped, query_bs._wrapped,
            extra_query_bs_array, extra_query_count, hints_bs._wrapped,
        )
        if ejq is None:
            queries = (query,) + queries
            raise CommandError(
                'Could not build query from {qs} with hints {hs}.'.format(
                    qs=queries, hs=hints,
                )
            )
        count = ctypes.c_uint32()
        tclist_p = c.ejdb.qryexecute(
            self._wrapped, ejq, ctypes.byref(count), flags, c.TCXSTRREF(0),
        )
        c.ejdb.querydel(ejq)
        return tclist_p, count.value

    def count(self, *queries, **kwargs):
        """count(*queries, hints={})

        Get the number of documents in this collection.

        :param hints: A mapping of possible hints to the selection.
        """
        # TODO: Document hints, implement MongoDB-like hinting kwargs.
        tclist_p, count = self._execute(queries, kwargs, flags=c.JBQRYCOUNT)
        return count

    def find_one(self, *queries, **kwargs):
        """find_one(*queries, hints={})

        Find a single document in the collection.

        :param hints: A mapping of possible hints to the selection.
        :returns: A mapping for the document found, or `None` if no matching
            document exists.
        """
        # TODO: Document hints, implement MongoDB-like hinting kwargs.
        # TODO: Add a flag to choose whether we should raise
        # DocumentDoesNotExist or return None with an empty result.
        hints = kwargs.pop('hints', {})
        tclist_p, count = self._execute(queries, hints, flags=c.JBQRYFINDONE)
        cursor = Cursor(wrapped=tclist_p, count=count)
        try:
            document = cursor[0]
        except IndexError:
            document = None
        return document

    def find(self, *queries, **kwargs):
        """find(*queries, hints={})

        Find documents in the collection.

        :param hints: A mapping of possible hints to the selection.
        :returns: A :class:`Cursor` instance corresponding to this query.
        """
        # TODO: Document hints, implement MongoDB-like hinting kwargs.
        hints = kwargs.pop('hints', {})
        tclist_p, count = self._execute(queries, hints, flags=0)
        return Cursor(wrapped=tclist_p, count=count)

    def delete_one(self, *queries, **kwargs):
        """delete_one(*queries, hints={})

        Delete a single document in the collection.

        This is an optimized shortcut for `find_one({..., '$dropall': True})`.
        Use the formal syntax if you want to get the deleted document's
        content.

        :param hints: A mapping of possible hints to the selection.
        :returns: A boolean specifying whether a document is deleted.
        """
        hints = kwargs.pop('hints', {})
        tclist_p, count = self._execute(
            queries, hints, flags=(c.JBQRYFINDONE | c.JBQRYCOUNT),
            query_items={'$dropall': True},
        )
        return bool(count)

    def delete_many(self, *queries, **kwargs):
        """delete_many(*queries, hints={})

        Delete documents in the collection.

        This is an optimized shortcut for `find({..., '$dropall': True})`.
        Use the formal syntax if you want to get the content of deleted
        documents.

        :param hints: A mapping of possible hints to the selection.
        :returns: Count of documents deleted.
        """
        hints = kwargs.pop('hints', {})
        tclist_p, count = self._execute(
            queries, hints, flags=c.JBQRYCOUNT,
            query_items={'$dropall': True},
        )
        return count

    def save(self, *documents, **kwargs):
        """save(*documents, merge=False)

        Persist one or more documents in the collection.

        If a saved document doesn't have a `_id` key, an automatically
        generated unused OID will be used. Otherwise the OID is set to the
        given document's `_id` field, possibly overwriting an existing document
        in the collection.

        This method is provided for compatibility with `ejdb-python`.

        :param merge: If evalutes to `True`, content of existing document with
            matching `_id` will be merged with the provided document's content.
        """
        merge = kwargs.pop('merge', False)
        with self.begin_transaction():
            for document in documents:
                oid = self._perform_save(document, merge)
                document.pop(bson.ID_KEY_NAME, None)
                document[c.JDBIDKEYNAME] = six.text_type(oid)

    def remove(self, oid):
        """Remove the document matching the given OID from the collection.

        This method is provided for compatibility with `ejdb-python`.
        """
        if not is_valid_oid(oid):
            raise ValueError('OID should be a 24-character-long hex string.')
        oid = c.BSONOID.from_string(oid)
        ok = c.ejdb.rmbson(self._wrapped, oid)
        if not ok:
            raise DatabaseError(_get_errmsg(self.database))

    def create_index(self, path, index_type):
        _set_index(self, 'add', path, index_type)

    def remove_index(self, path, index_type=None):
        """Remove index(es) on `path` from the collection.

        The index of specified type on `path`, if given by `index_type`, will
        be removed. If `index_type` is `None`, all indexes on `path` will be
        removed.
        """
        if index_type is None:
            ok = c.ejdb.setindex(
                self._wrapped, coerce_char_p(path), c.JBIDXDROPALL,
            )
            if not ok:
                raise DatabaseError(_get_errmsg(self.database))
        _set_index(self, 'remove', path, index_type, flags=c.JBIDXDROP)

    def rebuild_index(self, path, index_type):
        _set_index(self, 'rebuild', path, index_type, flags=c.JBIDXREBLD)

    def optimize_index(self, path, index_type):
        _set_index(self, 'optimize', path, index_type, flags=c.JBIDXOP)

    def create_string_index(self, path):
        self.create_index(path, STRING)

    def create_istring_index(self, path):
        self.create_index(path, ISTRING)

    def create_number_index(self, path):
        self.create_index(path, NUMBER)

    def create_array_index(self, path):
        self.create_index(path, ARRAY)

    def remove_string_index(self, path):
        self.remove_index(path, STRING)

    def remove_istring_index(self, path):
        self.remove_index(path, ISTRING)

    def remove_number_index(self, path):
        self.remove_index(path, NUMBER)

    def remove_array_index(self, path):
        self.remove_index(path, ARRAY)

    def rebuild_string_index(self, path):
        self.rebuild_index(path, STRING)

    def rebuild_istring_index(self, path):
        self.rebuild_index(path, ISTRING)

    def rebuild_number_index(self, path):
        self.rebuild_index(path, NUMBER)

    def rebuild_array_index(self, path):
        self.rebuild_index(path, ARRAY)

    def optimize_string_index(self, path):
        self.optimize_index(path, STRING)

    def optimize_istring_index(self, path):
        self.optimize_index(path, ISTRING)

    def optimize_number_index(self, path):
        self.optimize_index(path, NUMBER)

    def optimize_array_index(self, path):
        self.remove_index(path, ARRAY)


class CollectionIterator(tc.ListIterator):

    def __init__(self, database, wrapped):
        super(CollectionIterator, self).__init__(wrapped=wrapped)
        self._database = database

    def instantiate(self, value_p):
        name = coerce_str(ctypes.cast(value_p, c.EJCOLLREF).contents.cname)
        return self._database.get_collection(name)


class Database(CObjectWrapper):
    """Representation of an EJDB.

    A :class:`Database` instance can be created like this::

        db = ejdb.Database(
            path='path_to_db',
            options=(ejdb.WRITE | ejdb.TRUNCATE),
        )

    The database is opened immediately, unless the `path` argument evalutes to
    `False`. In such cases the user needs to set the path and manually call
    :func:`open` later.
    """
    @_init_c
    def __init__(self, path='', options=READ):
        """__init__(path='', options=READ)
        """
        super(Database, self).__init__(
            wrapped=c.ejdb.new(), finalizer=_ejdb_finalizer,
        )
        self._path = coerce_str(path)
        self._options = options
        if self.path:
            self.open()

    def __contains__(self, name):
        return self.has_collection(name)

    def __getitem__(self, name):
        return self.get_collection(name)

    def __iter__(self):
        tclist_p = c.ejdb.getcolls(self._wrapped)
        return CollectionIterator(database=self, wrapped=tclist_p)

    def __len__(self):
        tclist_p = c.ejdb.getcolls(self._wrapped)
        num = c.tc.listnum(tclist_p)
        c.tc.listdel(tclist_p)
        return num

    def __enter__(self):
        # Don't need to do anything since the session has already begun.
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def path(self):
        """Path to the EJDB.

        This can be modified if the database instance is not opened.
        """
        return self._path

    @path.setter
    def path(self, path):
        if self.is_open():
            raise DatabaseError('Could not set path to an open database.')
        self._path = path

    @property
    def options(self):
        """Options for the EJDB.

        This can be modified if the database instance is not opened.
        """
        return self._options

    @options.setter
    def options(self, options):
        if self.is_open():
            raise DatabaseError('Could not set options to an open database.')
        self._options = options

    @property
    def writable(self):
        return bool(self.options & WRITE)

    @property
    def collections(self):
        return {collection for collection in self}

    @property
    def collection_names(self):
        return {collection.name for collection in self}

    # def version(self):
    #     """Get format version of the current database.
    #
    #     Returns a three-tuple of integers representing the major, minor, and
    #     patch version numbers. Raises `DatabaseError` if the database is not
    #     opened.
    #     """
    #     if not self.is_open():
    #         raise DatabaseError(
    #             'Could not read format version of a closed database.'
    #         )
    #     ver = c.ejdb.formatversion()
    #     patch = ver % 0x100
    #     ver //= 0x100
    #     minor = ver % 0x100
    #     major = ver // 0x100
    #     return (major, minor, patch,)

    def open(self):
        """Open this EJDB.

        This can be used directly, with the user calling :func:`close` later
        manually::

            db.open()
            try:
                ... # Do things.
            except:
                ... # Handle exceptions.
            finally:
                db.close()

        Or as a context manager::

            with db.open():
                ... # Do things.

        In the latter usage, :func:`close` will be called automatically when
        the block exits.
        """
        # Open the database immediately, so that direct call on
        # `Database.open()` works without a `with` block.
        if self.is_open():
            raise DatabaseError('Database already opened.')

        path = coerce_char_p(self.path)
        ok = c.ejdb.open(self._wrapped, path, self.options)
        if not ok:
            raise DatabaseError(_get_errmsg(self))

    def close(self):
        """Close this EJDB.
        """
        if not self.is_open():
            raise DatabaseError('Database not opened.')
        ok = c.ejdb.close(self._wrapped)
        if not ok:  # pragma: no cover
            raise DatabaseError(_get_errmsg(self))

    def is_open(self):
        """Check whether this EJDB is currently open.
        """
        open_state = c.ejdb.isopen(self._wrapped)
        return open_state

    def create_collection(self, name, exist_ok=False, **options):
        """Create a collection in this database with given options.

        The newly-created collection is returned. If `exist_ok` is `True`,
        existing collection with the same name will be returned, otherwise an
        error will be raised.

        Options only apply to newly-created collection. Existing collections
        will not be affected. Possible options include:

        :param large: If `True`, the collection can be larger than 2 GB.
            Default is `False`.
        :param compressed: If `True`, the collection will be compressed with
            DEFLATE compression. Default is `False`.
        :param records: Expected records number in the collection. Default is
            `128000`.
        :param cachedrecords: Maximum number of records cached in memory.
            Default is `0`.
        """
        c_name = coerce_char_p(name)
        if not exist_ok and c.ejdb.getcoll(self._wrapped, c_name):
            raise DatabaseError(
                "Collection with name '{name}' already exists.".format(
                    name=name,
                )
            )
        if options is None:
            options = {}
        ejcollopts = c.EJCOLLOPTS(**options)
        wrapped = c.ejdb.createcoll(self._wrapped, c_name, ejcollopts)
        if not wrapped:     # pragma: no cover
            raise DatabaseError(_get_errmsg(self))
        return Collection(database=self, wrapped=wrapped)

    def drop_collection(self, name, unlink=True):
        """Drop a collection in this database.

        Does nothing if a database with matching name does not exist.

        :param name: Name of collection to drop.
        :param unlink: If `True`, removes all related index and collection
            files. Default is `True`.
        """
        c_name = coerce_char_p(name)
        ok = c.ejdb.rmcoll(self._wrapped, c_name, unlink)
        if not ok:  # pragma: no cover
            raise DatabaseError(_get_errmsg(self))

    def get_collection(self, name):
        """Get the collection with name `name` inside this EJDB.
        """
        c_name = coerce_char_p(name)
        wrapped = c.ejdb.getcoll(self._wrapped, c_name)
        if not wrapped:
            raise CollectionDoesNotExist(name)
        return Collection(database=self, wrapped=wrapped)

    def has_collection(self, name):
        """Check whether this EJDB contains a collection named `name`.
        """
        c_name = coerce_char_p(name)
        return bool(c.ejdb.getcoll(self._wrapped, c_name))

    def find(self, collection_name, *args, **kwargs):
        """find(collection_name, *queries, hints={})

        Shortcut to query a collection in the database.

        The following usage::

            db.find('people', {'name': 'TP'})

        is semantically identical to::

            collection = db.create_collection('people', exist_ok=True)
            collection.find({'name': 'TP'})
        """
        coll = self.create_collection(collection_name, exist_ok=True)
        return coll.find(*args, **kwargs)

    def find_one(self, collection_name, *args, **kwargs):
        """find_one(collection_name, *queries, hints={})

        Shortcut to query a collection in the database.

        The following usage::

            db.find_one('people', {'name': 'TP'})

        is semantically identical to::

            collection = db.create_collection('people', exist_ok=True)
            collection.find_one({'name': 'TP'})
        """
        coll = self.create_collection(collection_name, exist_ok=True)
        return coll.find_one(*args, **kwargs)

    def save(self, collection_name, *args, **kwargs):
        """save(collection_name, *documents, merge=False)

        Shortcut to save to a collection in the database.

        The following usage::

            db.save({'people', {'name': 'TP'})

        is semantically identical to::

            collection = db.create_collection('people', exist_ok=True)
            collection.save({'name': 'TP'})
        """
        coll = self.create_collection(collection_name, exist_ok=True)
        return coll.save(*args, **kwargs)

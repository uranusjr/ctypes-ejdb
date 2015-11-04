#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Provide constants and composite types for binding with ctypes.
"""

from __future__ import absolute_import
import ctypes
import ctypes.util
import os

from .utils import coerce_char_p, python_2_unicode_compatible, read_ejdb_config


JDBIDKEYNAME = '_id'

# enum { /** Database open modes */
#     JBOREADER = 1u << 0,    /**< Open as a reader. */
#     JBOWRITER = 1u << 1,    /**< Open as a writer. */
#     JBOCREAT = 1u << 2,     /**< Create if db file not exists. */
#     JBOTRUNC = 1u << 3,     /**< Truncate db on open. */
#     JBONOLCK = 1u << 4,     /**< Open without locking. */
#     JBOLCKNB = 1u << 5,     /**< Lock without blocking. */
#     JBOTSYNC = 1u << 6      /**< Synchronize every transaction. */
# };
JBOREADER = 1 << 0
JBOWRITER = 1 << 1
JBOCREAT = 1 << 2
JBOTRUNC = 1 << 3
JBONOLCK = 1 << 4
JBOLCKNB = 1 << 5
JBOTSYNC = 1 << 5


# enum { /** Index modes, index types. */
#     JBIDXDROP = 1 << 0, /**< Drop index. */
#     JBIDXDROPALL = 1 << 1, /**< Drop index for all types. */
#     JBIDXOP = 1 << 2, /**< Optimize indexes. */
#     JBIDXREBLD = 1 << 3, /**< Rebuild index. */
#     JBIDXNUM = 1 << 4, /**< Number index. */
#     JBIDXSTR = 1 << 5, /**< String index.*/
#     JBIDXARR = 1 << 6, /**< Array token index. */
#     JBIDXISTR = 1 << 7 /**< Case insensitive string index */
# };
JBIDXDROP = 1 << 0
JBIDXDROPALL = 1 << 1
JBIDXOP = 1 << 2
JBIDXREBLD = 1 << 3
JBIDXNUM = 1 << 4
JBIDXSTR = 1 << 5
JBIDXARR = 1 << 6
JBIDXISTR = 1 << 7


# enum { /*< Query search mode flags in ejdbqryexecute() */
#     JBQRYCOUNT = 1, /*< Query only count(*) */
#     JBQRYFINDONE = 1 << 1 /*< Fetch first record only */
# };
JBQRYCOUNT = 1
JBQRYFINDONE = 1 << 1


# #define BSON_OK 0
# #define BSON_ERROR -1
BSON_OK = 0
BSON_ERROR = -1


# typedef enum {
#     BSON_EOO = 0,
#     BSON_DOUBLE = 1,
#     BSON_STRING = 2,
#     BSON_OBJECT = 3,
#     BSON_ARRAY = 4,
#     BSON_BINDATA = 5,
#     BSON_UNDEFINED = 6,
#     BSON_OID = 7,
#     BSON_BOOL = 8,
#     BSON_DATE = 9,
#     BSON_NULL = 10,
#     BSON_REGEX = 11,
#     BSON_DBREF = 12, /**< Deprecated. */
#     BSON_CODE = 13,
#     BSON_SYMBOL = 14,
#     BSON_CODEWSCOPE = 15,
#     BSON_INT = 16,
#     BSON_TIMESTAMP = 17,
#     BSON_LONG = 18
# } bson_type;
BSON_EOO = 0     # End of object.
BSON_DOUBLE = 1
BSON_STRING = 2
BSON_OBJECT = 3
BSON_ARRAY = 4
BSON_BINDATA = 5
BSON_UNDEFINED = 6
BSON_OID = 7
BSON_BOOL = 8
BSON_DATE = 9
BSON_NULL = 10
BSON_REGEX = 11
BSON_DBREF = 12
BSON_CODE = 13
BSON_SYMBOL = 14
BSON_CODEWSCOPE = 15
BSON_INT = 16
BSON_TIMESTAMP = 17
BSON_LONG = 18


# enum bson_binary_subtype_t {
#     BSON_BIN_BINARY = 0,
#     BSON_BIN_FUNC = 1,
#     BSON_BIN_BINARY_OLD = 2,
#     BSON_BIN_UUID = 3,
#     BSON_BIN_MD5 = 5,
#     BSON_BIN_USER = 128
# };
BSON_BIN_BINARY = 0
BSON_BIN_FUNC = 1
BSON_BIN_BINARY_OLD = 2
BSON_BIN_UUID = 3
BSON_BIN_MD5 = 5
BSON_BIN_USER = 128


# We treat these constructs as opaque pointers.
BSONREF = ctypes.c_void_p
BSONITERREF = ctypes.c_void_p
EJDBREF = ctypes.c_void_p
EJQREF = ctypes.c_void_p
TCLISTREF = ctypes.c_void_p
TCXSTRREF = ctypes.c_void_p

EJQRESULT = TCLISTREF


# struct EJCOLL { /**> EJDB Collection. */
#     char *cname; /**> Collection name. */
#     int cnamesz; /**> Collection name length. */
#     TCTDB *tdb; /**> Collection TCTDB. */
#     EJDB *jb; /**> Database handle. */
#     void *mmtx; /*> Mutex for method */
# };
# TODO: This is private API. Find a way to retrieve cname without this.
class EJCOLL(ctypes.Structure):
    _fields_ = [
        ('cname', ctypes.c_char_p),
        ('cnamesz', ctypes.c_int),
        ('tdb', ctypes.c_void_p),
        ('jb', EJDBREF),
        ('mmtx', ctypes.c_void_p),
    ]


EJCOLLREF = ctypes.POINTER(EJCOLL)


# typedef struct {        /**< EJDB collection tuning options. */
#     bool large;
#       /**< Large collection. It can be larger than 2GB. Default false */
#     bool compressed;
#       /**< Collection records will be compressed with DEFLATE compression.
#            Default: false */
#     int64_t records;
#       /**< Expected records number in the collection. Default: 128K */
#     int cachedrecords;
#       /**< Maximum number of records cached in memory. Default: 0 */
# } EJCOLLOPTS;
class EJCOLLOPTS(ctypes.Structure):
    _fields_ = [
        ('large', ctypes.c_bool),
        ('compressed', ctypes.c_bool),
        ('records', ctypes.c_int64),
        ('cachedrecords', ctypes.c_int),
    ]


EJCOLLOPTSREF = ctypes.POINTER(EJCOLLOPTS)


# #pragma pack(1)
# typedef union {
#     char bytes[12];
#     int ints[3];
# } bson_oid_t;
# #pragma pack()
@python_2_unicode_compatible
class BSONOID(ctypes.Union):

    _pack_ = 1
    _fields_ = [
        ('bytes', ctypes.c_char * 12),
        ('ints', ctypes.c_int * 3),
    ]

    def __str__(self):
        buf = ctypes.create_string_buffer(25)
        bson.oid_to_string(ctypes.byref(self), buf)
        s = buf.value.decode('ascii')   # ASCII is enough since OID is hex.
        return s

    @classmethod
    def from_string(cls, s):
        s = coerce_char_p(s)
        if not ejdb.isvalidoidstr(s):
            raise ValueError('OID should be a 24-character-long hex string.')
        oid = cls()
        bson.oid_from_string(ctypes.byref(oid), s)
        return oid


BSONOIDREF = ctypes.POINTER(BSONOID)


class Lib(object):
    pass


# Will contain C functions after initialization.
ejdb = Lib()
bson = Lib()
tc = Lib()

initialized = False


def init(ejdb_path=None):
    ejdb_path = (
        ejdb_path
        or os.environ.get('CTYPES_EJDB_PATH')
        or read_ejdb_config()
        or ctypes.util.find_library('ejdb')
    )

    if not ejdb_path:   # pragma: no cover
        raise RuntimeError('EJDB binary not found')

    # Access to the C library.
    _ = ctypes.cdll.LoadLibrary(ejdb_path)

    ejdb.version = _.ejdbversion
    ejdb.version.argtypes = []
    ejdb.version.restype = ctypes.c_char_p

    # TODO: Expose `ejdbformatversion` as a tuple (int, int, int) when it's
    # available.

    ejdb.isvalidoidstr = _.ejdbisvalidoidstr
    ejdb.isvalidoidstr.argtypes = [ctypes.c_char_p]
    ejdb.isvalidoidstr.restype = ctypes.c_bool

    ejdb.ecode = _.ejdbecode
    ejdb.ecode.argtypes = [EJDBREF]
    ejdb.ecode.restype = ctypes.c_int

    ejdb.errmsg = _.ejdberrmsg
    ejdb.errmsg.argtypes = [ctypes.c_int]
    ejdb.errmsg.restype = ctypes.c_char_p

    ejdb.del_ = _.ejdbdel
    ejdb.del_.argtypes = [EJDBREF]
    ejdb.del_.restype = None

    ejdb.new = _.ejdbnew
    ejdb.new.argtypes = []
    ejdb.new.restype = EJDBREF

    ejdb.close = _.ejdbclose
    ejdb.close.argtypes = [EJDBREF]
    ejdb.close.restype = ctypes.c_bool

    ejdb.open = _.ejdbopen
    ejdb.open.argtypes = [EJDBREF, ctypes.c_char_p, ctypes.c_int]
    ejdb.open.restype = ctypes.c_bool

    ejdb.isopen = _.ejdbisopen
    ejdb.isopen.argtypes = [EJDBREF]
    ejdb.isopen.restype = ctypes.c_bool

    #

    ejdb.getcoll = _.ejdbgetcoll
    ejdb.getcoll.argtypes = [EJDBREF, ctypes.c_char_p]
    ejdb.getcoll.restype = EJCOLLREF

    ejdb.getcolls = _.ejdbgetcolls
    ejdb.getcolls.argtypes = [EJDBREF]
    ejdb.getcolls.restype = ctypes.c_void_p

    ejdb.createcoll = _.ejdbcreatecoll
    ejdb.createcoll.argtypes = [EJDBREF, ctypes.c_char_p, EJCOLLOPTSREF]
    ejdb.createcoll.restype = EJCOLLREF

    ejdb.rmcoll = _.ejdbrmcoll
    ejdb.rmcoll.argtypes = [EJDBREF, ctypes.c_char_p, ctypes.c_bool]
    ejdb.rmcoll.restype = ctypes.c_bool

    ejdb.savebson2 = _.ejdbsavebson2
    ejdb.savebson2.argtypes = [EJCOLLREF, BSONREF, BSONOIDREF, ctypes.c_bool]
    ejdb.savebson2.restype = ctypes.c_bool

    ejdb.rmbson = _.ejdbrmbson
    ejdb.rmbson.argtypes = [EJCOLLREF, BSONOIDREF]
    ejdb.rmbson.restype = ctypes.c_bool

    ejdb.loadbson = _.ejdbloadbson
    ejdb.loadbson.argtypes = [EJCOLLREF, BSONOIDREF]
    ejdb.loadbson.restype = BSONREF

    ejdb.setindex = _.ejdbsetindex
    ejdb.setindex.argtypes = [EJCOLLREF, ctypes.c_char_p, ctypes.c_int]
    ejdb.setindex.restype = ctypes.c_bool

    ejdb.meta = _.ejdbmeta
    ejdb.meta.argtypes = [EJDBREF]
    ejdb.meta.restype = BSONREF

    #

    ejdb.createquery = _.ejdbcreatequery
    ejdb.createquery.argtypes = [
        EJDBREF, BSONREF, BSONREF, ctypes.c_int, BSONREF,
    ]
    ejdb.createquery.restype = EJQREF

    ejdb.querydel = _.ejdbquerydel
    ejdb.querydel.argtypes = [EJQREF]
    ejdb.querydel.restype = None

    ejdb.qryexecute = _.ejdbqryexecute
    ejdb.qryexecute.argtypes = [
        EJCOLLREF,
        EJQREF,         # The query.
        ctypes.POINTER(ctypes.c_uint32),    # Will hold the output count.
        ctypes.c_int,   # If set to `JBQRYCOUNT`, only performs counting.
        TCXSTRREF,      # Optional debug logging output.
    ]
    ejdb.qryexecute.restype = EJQRESULT

    #

    ejdb.tranbegin = _.ejdbtranbegin
    ejdb.tranbegin.argtypes = [EJCOLLREF]
    ejdb.tranbegin.restype = ctypes.c_bool

    ejdb.trancommit = _.ejdbtrancommit
    ejdb.trancommit.argtypes = [EJCOLLREF]
    ejdb.trancommit.restype = ctypes.c_bool

    ejdb.tranabort = _.ejdbtranabort
    ejdb.tranabort.argtypes = [EJCOLLREF]
    ejdb.tranabort.restype = ctypes.c_bool

    ejdb.transtatus = _.ejdbtranstatus
    ejdb.transtatus.argtypes = [EJCOLLREF, ctypes.POINTER(ctypes.c_bool)]
    ejdb.transtatus.restype = ctypes.c_bool

    ejdb.syncdb = _.ejdbsyncdb
    ejdb.syncdb.argtypes = [EJDBREF]
    ejdb.syncdb.restype = ctypes.c_bool

    tc.listdel = _.tclistdel
    tc.listdel.argtypes = [TCLISTREF]
    tc.listdel.restype = None

    tc.listnum = _.tclistnum
    tc.listnum.argtypes = [TCLISTREF]
    tc.listnum.restype = ctypes.c_int

    # Return type in the original tcutil.h declaration is char *, but it really
    # is a data array, so we use c_void_p here to prevent Python casting it to
    # bytes. Consumer of this method should use ctypes.string_at or ctypes.cast
    # to get the content.
    tc.listval2 = _.tclistval2
    tc.listval2.argtypes = [TCLISTREF, ctypes.c_int]
    tc.listval2.restype = ctypes.c_void_p

    bson.create = _.bson_create
    bson.create.argtypes = []
    bson.create.restype = BSONREF

    bson.del_ = _.bson_del
    bson.del_.argtypes = [BSONREF]
    bson.del_.restype = None

    bson.init = _.bson_init
    bson.init.argtypes = [BSONREF]
    bson.init.restype = None

    bson.init_as_query = _.bson_init_as_query
    bson.init_as_query.argtypes = [BSONREF]
    bson.init_as_query.restype = None

    # Second arg type in the original bson.h declaration is char *, but it
    # really is a data pointer, so we use c_void_p here.
    bson.init_on_stack = _.bson_init_on_stack
    bson.init_on_stack.argtypes = [
        BSONREF, ctypes.c_void_p, ctypes.c_int, ctypes.c_int,
    ]
    bson.init_on_stack.restype = None

    bson.finish = _.bson_finish
    bson.finish.argtypes = [BSONREF]
    bson.finish.restype = ctypes.c_int

    bson.append_oid = _.bson_append_oid
    bson.append_oid.argtypes = [BSONREF, ctypes.c_char_p, BSONOIDREF]
    bson.append_oid.restype = ctypes.c_int

    bson.append_int = _.bson_append_int
    bson.append_int.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_int]
    bson.append_int.restype = ctypes.c_int

    bson.append_long = _.bson_append_long
    bson.append_long.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_int64]
    bson.append_long.restype = ctypes.c_int

    bson.append_double = _.bson_append_double
    bson.append_double.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_double]
    bson.append_double.restype = ctypes.c_int

    bson.append_string_n = _.bson_append_string_n
    bson.append_string_n.argtypes = [
        BSONREF, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int,
    ]
    bson.append_string_n.restype = ctypes.c_int

    # Type of the third argument in the original bson.h declaration is char,
    # but it really is an enum value, so we use c_int instead.
    bson.append_binary = _.bson_append_binary
    bson.append_binary.argtypes = [
        BSONREF, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
    ]
    bson.append_binary.restype = ctypes.c_int

    bson.append_bool = _.bson_append_bool
    bson.append_bool.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_bool]
    bson.append_bool.restype = ctypes.c_int

    bson.append_null = _.bson_append_null
    bson.append_null.argtypes = [BSONREF, ctypes.c_char_p]
    bson.append_null.restype = ctypes.c_int

    bson.append_undefined = _.bson_append_undefined
    bson.append_undefined.argtypes = [BSONREF, ctypes.c_char_p]
    bson.append_undefined.restype = ctypes.c_int

    bson.append_regex = _.bson_append_regex
    bson.append_regex.argtypes = [
        BSONREF, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int,
    ]
    bson.append_regex.restype = ctypes.c_int

    bson.append_date = _.bson_append_date
    bson.append_date.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_int64]
    bson.append_date.restype = ctypes.c_int

    bson.append_start_array = _.bson_append_start_array
    bson.append_start_array.argtypes = [BSONREF, ctypes.c_char_p]
    bson.append_start_array.restype = ctypes.c_int

    bson.append_finish_array = _.bson_append_finish_array
    bson.append_finish_array.argtypes = [BSONREF]
    bson.append_finish_array.restype = ctypes.c_int

    bson.append_start_object = _.bson_append_start_object
    bson.append_start_object.argtypes = [BSONREF, ctypes.c_char_p]
    bson.append_start_object.restype = ctypes.c_int

    bson.append_finish_object = _.bson_append_finish_object
    bson.append_finish_object.argtypes = [BSONREF]
    bson.append_finish_object.restype = ctypes.c_int

    # Return type in the original bson.h declaration is char *, but it really
    # is a data array, so we use c_void_p here to prevent Python casting it to
    # bytes. Consumer should use ctypes.string_at to get content.
    bson.data2 = _.bson_data2
    bson.data2.argtypes = [BSONREF, ctypes.POINTER(ctypes.c_int)]
    bson.data2.restype = ctypes.c_void_p

    bson.size2 = _.bson_size2
    bson.size2.argtypes = [ctypes.c_void_p]
    bson.size2.restype = ctypes.c_int

    bson.oid_from_string = _.bson_oid_from_string
    bson.oid_from_string.argtypes = [BSONOIDREF, ctypes.c_char_p]
    bson.oid_from_string.restype = None

    bson.oid_to_string = _.bson_oid_to_string
    bson.oid_to_string.argtypes = [BSONOIDREF, ctypes.c_char_p]
    bson.oid_to_string.restype = None

    bson.iterator_create = _.bson_iterator_create
    bson.iterator_create.argtypes = []
    bson.iterator_create.restype = BSONITERREF

    bson.iterator_dispose = _.bson_iterator_dispose
    bson.iterator_dispose.argtypes = [BSONITERREF]
    bson.iterator_dispose.restype = None

    bson.iterator_init = _.bson_iterator_init
    bson.iterator_init.argtypes = [BSONITERREF, BSONREF]
    bson.iterator_init.restype = None

    bson.iterator_next = _.bson_iterator_next
    bson.iterator_next.argtypes = [BSONITERREF]
    bson.iterator_next.restype = ctypes.c_int

    bson.iterator_key = _.bson_iterator_key
    bson.iterator_key.argtypes = [BSONITERREF]
    bson.iterator_key.restype = ctypes.c_char_p

    bson.iterator_double_raw = _.bson_iterator_double_raw
    bson.iterator_double_raw.argtypes = [BSONITERREF]
    bson.iterator_double_raw.restype = ctypes.c_double

    bson.iterator_int_raw = _.bson_iterator_int_raw
    bson.iterator_int_raw.argtypes = [BSONITERREF]
    bson.iterator_int_raw.restype = ctypes.c_int

    bson.iterator_long_raw = _.bson_iterator_long_raw
    bson.iterator_long_raw.argtypes = [BSONITERREF]
    bson.iterator_long_raw.restype = ctypes.c_int64

    bson.iterator_bool_raw = _.bson_iterator_bool_raw
    bson.iterator_bool_raw.argtypes = [BSONITERREF]
    bson.iterator_bool_raw.restype = ctypes.c_bool

    bson.iterator_oid = _.bson_iterator_oid
    bson.iterator_oid.argtypes = [BSONITERREF]
    bson.iterator_oid.restype = BSONOIDREF

    # Return type in the original bson.h declaration is char *, but it really
    # is a data array, so we use c_void_p here to prevent Python casting it to
    # bytes. Consumer should use ctypes.string_at to get content.
    bson.iterator_string = _.bson_iterator_string
    bson.iterator_string.argtypes = [BSONITERREF]
    bson.iterator_string.restype = ctypes.c_void_p

    bson.iterator_string_len = _.bson_iterator_string_len
    bson.iterator_string_len.argtypes = [BSONITERREF]
    bson.iterator_string_len.restype = ctypes.c_int

    bson.iterator_bin_len = _.bson_iterator_bin_len
    bson.iterator_bin_len.argtypes = [BSONITERREF]
    bson.iterator_bin_len.restype = ctypes.c_int

    # Return type in the original bson.h declaration is char, but it really is
    # an enum value, so we use c_int instead.
    bson.iterator_bin_type = _.bson_iterator_bin_type
    bson.iterator_bin_type.argtypes = [BSONITERREF]
    bson.iterator_bin_type.restype = ctypes.c_int

    # Return type in the original bson.h declaration is char *, but it really
    # is a data array, so we use c_void_p here to prevent Python casting it to
    # bytes. Consumer should use ctypes.string_at to get content.
    bson.iterator_bin_data = _.bson_iterator_bin_data
    bson.iterator_bin_data.argtypes = [BSONITERREF]
    bson.iterator_bin_data.restype = ctypes.c_void_p

    bson.iterator_date = _.bson_iterator_date
    bson.iterator_date.argtypes = [BSONITERREF]
    bson.iterator_date.restype = ctypes.c_int64

    bson.iterator_subiterator = _.bson_iterator_subiterator
    bson.iterator_subiterator.argtypes = [BSONITERREF, BSONITERREF]
    bson.iterator_subiterator.restype = None

    # For debugging.
    bson.print_raw = _.bson_print_raw
    bson.print_raw.argtypes = [ctypes.c_char_p, ctypes.c_int]
    bson.print_raw.restype = None

    global initialized
    initialized = True

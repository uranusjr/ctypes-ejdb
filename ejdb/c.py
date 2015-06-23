#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Provide constants and composite types for binding with ctypes.
"""

from __future__ import absolute_import
import ctypes
import ctypes.util

from .utils import coerce_char_p, python_2_unicode_compatible


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
        buf = ctypes.create_string_buffer(24)
        bson_oid_to_string(ctypes.byref(self), buf)
        s = buf.value.decode('ascii')   # ASCII is enough since OID is hex.
        return s

    @classmethod
    def from_string(cls, s):
        s = coerce_char_p(s)
        if not ejdbisvalidoidstr(s):
            raise ValueError('OID should be a 24-character-long hex string.')
        oid = cls()
        bson_oid_from_string(ctypes.byref(oid), s)
        return oid


BSONOIDREF = ctypes.POINTER(BSONOID)


# Access to the C library.
_ = ctypes.cdll.LoadLibrary(ctypes.util.find_library('ejdb'))

ejdbversion = _.ejdbversion
ejdbversion.argtypes = []
ejdbversion.restype = ctypes.c_char_p

# TODO: Expose `ejdbformatversion` as a tuple (int, int, int) when it's
# available.

ejdbisvalidoidstr = _.ejdbisvalidoidstr
ejdbisvalidoidstr.argtypes = [ctypes.c_char_p]
ejdbisvalidoidstr.restype = ctypes.c_bool

ejdbecode = _.ejdbecode
ejdbecode.argtypes = [EJDBREF]
ejdbecode.restype = ctypes.c_int

ejdberrmsg = _.ejdberrmsg
ejdberrmsg.argtypes = [ctypes.c_int]
ejdberrmsg.restype = ctypes.c_char_p

ejdbdel = _.ejdbdel
ejdbdel.argtypes = [EJDBREF]
ejdbdel.restype = None

ejdbnew = _.ejdbnew
ejdbnew.argtypes = []
ejdbnew.restype = EJDBREF

ejdbclose = _.ejdbclose
ejdbclose.argtypes = [EJDBREF]
ejdbclose.restype = ctypes.c_bool

ejdbopen = _.ejdbopen
ejdbopen.argtypes = [EJDBREF, ctypes.c_char_p, ctypes.c_int]
ejdbopen.restype = ctypes.c_bool

ejdbisopen = _.ejdbisopen
ejdbisopen.argtypes = [EJDBREF]
ejdbisopen.restype = ctypes.c_bool


ejdbgetcoll = _.ejdbgetcoll
ejdbgetcoll.argtypes = [EJDBREF, ctypes.c_char_p]
ejdbgetcoll.restype = EJCOLLREF

ejdbgetcolls = _.ejdbgetcolls
ejdbgetcolls.argtypes = [EJDBREF]
ejdbgetcolls.restype = ctypes.c_void_p

ejdbcreatecoll = _.ejdbcreatecoll
ejdbcreatecoll.argtypes = [EJDBREF, ctypes.c_char_p, EJCOLLOPTSREF]
ejdbcreatecoll.restype = EJCOLLREF

ejdbrmcoll = _.ejdbrmcoll
ejdbrmcoll.argtypes = [EJDBREF, ctypes.c_char_p, ctypes.c_bool]
ejdbrmcoll.restype = ctypes.c_bool

ejdbsavebson2 = _.ejdbsavebson2
ejdbsavebson2.argtypes = [EJCOLLREF, BSONREF, BSONOIDREF, ctypes.c_bool]
ejdbsavebson2.restype = ctypes.c_bool

ejdbrmbson = _.ejdbrmbson
ejdbrmbson.argtypes = [EJCOLLREF, BSONOIDREF]
ejdbrmbson.restype = ctypes.c_bool

ejdbloadbson = _.ejdbloadbson
ejdbloadbson.argtypes = [EJCOLLREF, BSONOIDREF]
ejdbloadbson.restype = BSONREF

ejdbsetindex = _.ejdbsetindex
ejdbsetindex.argtypes = [EJCOLLREF, ctypes.c_char_p, ctypes.c_int]
ejdbsetindex.restype = ctypes.c_bool

ejdbmeta = _.ejdbmeta
ejdbmeta.argtypes = [EJDBREF]
ejdbmeta.restype = BSONREF


ejdbcreatequery = _.ejdbcreatequery
ejdbcreatequery.argtypes = [EJDBREF, BSONREF, BSONREF, ctypes.c_int, BSONREF]
ejdbcreatequery.restype = EJQREF

ejdbquerydel = _.ejdbquerydel
ejdbquerydel.argtypes = [EJQREF]
ejdbquerydel.restype = None

ejdbqryexecute = _.ejdbqryexecute
ejdbqryexecute.argtypes = [
    EJCOLLREF,
    EJQREF,         # The query.
    ctypes.POINTER(ctypes.c_uint32),    # Will hold the output count.
    ctypes.c_int,   # If set to `JBQRYCOUNT`, only performs counting.
    TCXSTRREF,      # Optional debug logging output.
]
ejdbqryexecute.restype = EJQRESULT


ejdbtranbegin = _.ejdbtranbegin
ejdbtranbegin.argtypes = [EJCOLLREF]
ejdbtranbegin.restype = ctypes.c_bool

ejdbtrancommit = _.ejdbtrancommit
ejdbtrancommit.argtypes = [EJCOLLREF]
ejdbtrancommit.restype = ctypes.c_bool

ejdbtranabort = _.ejdbtranabort
ejdbtranabort.argtypes = [EJCOLLREF]
ejdbtranabort.restype = ctypes.c_bool

ejdbtranstatus = _.ejdbtranstatus
ejdbtranstatus.argtypes = [EJCOLLREF, ctypes.POINTER(ctypes.c_bool)]
ejdbtranstatus.restype = ctypes.c_bool

ejdbsyncdb = _.ejdbsyncdb
ejdbsyncdb.argtypes = [EJDBREF]
ejdbsyncdb.restype = ctypes.c_bool


tclistdel = _.tclistdel
tclistdel.argtypes = [TCLISTREF]
tclistdel.restype = None

tclistnum = _.tclistnum
tclistnum.argtypes = [TCLISTREF]
tclistnum.restype = ctypes.c_int

# Return type in the original tcutil.h declaration is char *, but it really is
# a data array, so we use c_void_p here to prevent Python casting it to bytes.
# Consumer of this method should use ctypes.string_at or ctypes.cast to get the
# content.
tclistval2 = _.tclistval2
tclistval2.argtypes = [TCLISTREF, ctypes.c_int]
tclistval2.restype = ctypes.c_void_p


bson_create = _.bson_create
bson_create.argtypes = []
bson_create.restype = BSONREF

bson_del = _.bson_del
bson_del.argtypes = [BSONREF]
bson_del.restype = None

bson_init = _.bson_init
bson_init.argtypes = [BSONREF]
bson_init.restype = None

bson_init_as_query = _.bson_init_as_query
bson_init_as_query.argtypes = [BSONREF]
bson_init_as_query.restype = None

# Second arg type in the original bson.h declaration is char *, but it really
# is a data pointer, so we use c_void_p here.
bson_init_on_stack = _.bson_init_on_stack
bson_init_on_stack.argtypes = [
    BSONREF, ctypes.c_void_p, ctypes.c_int, ctypes.c_int,
]
bson_init_on_stack.restype = None

bson_finish = _.bson_finish
bson_finish.argtypes = [BSONREF]
bson_finish.restype = ctypes.c_int

bson_append_oid = _.bson_append_oid
bson_append_oid.argtypes = [BSONREF, ctypes.c_char_p, BSONOIDREF]
bson_append_oid.restype = ctypes.c_int

bson_append_int = _.bson_append_int
bson_append_int.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_int]
bson_append_int.restype = ctypes.c_int

bson_append_long = _.bson_append_long
bson_append_long.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_int64]
bson_append_long.restype = ctypes.c_int

bson_append_double = _.bson_append_double
bson_append_double.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_double]
bson_append_double.restype = ctypes.c_int

bson_append_string_n = _.bson_append_string_n
bson_append_string_n.argtypes = [
    BSONREF, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int,
]
bson_append_string_n.restype = ctypes.c_int

# Type of the third argument in the original bson.h declaration is char, but it
# really is an enum value, so we use c_int instead.
bson_append_binary = _.bson_append_binary
bson_append_binary.argtypes = [
    BSONREF, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
]
bson_append_binary.restype = ctypes.c_int

bson_append_bool = _.bson_append_bool
bson_append_bool.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_bool]
bson_append_bool.restype = ctypes.c_int

bson_append_null = _.bson_append_null
bson_append_null.argtypes = [BSONREF, ctypes.c_char_p]
bson_append_null.restype = ctypes.c_int

bson_append_undefined = _.bson_append_undefined
bson_append_undefined.argtypes = [BSONREF, ctypes.c_char_p]
bson_append_undefined.restype = ctypes.c_int

bson_append_regex = _.bson_append_regex
bson_append_regex.argtypes = [
    BSONREF, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int,
]
bson_append_regex.restype = ctypes.c_int

bson_append_date = _.bson_append_date
bson_append_date.argtypes = [BSONREF, ctypes.c_char_p, ctypes.c_int64]
bson_append_date.restype = ctypes.c_int

bson_append_start_array = _.bson_append_start_array
bson_append_start_array.argtypes = [BSONREF, ctypes.c_char_p]
bson_append_start_array.restype = ctypes.c_int

bson_append_finish_array = _.bson_append_finish_array
bson_append_finish_array.argtypes = [BSONREF]
bson_append_finish_array.restype = ctypes.c_int

bson_append_start_object = _.bson_append_start_object
bson_append_start_object.argtypes = [BSONREF, ctypes.c_char_p]
bson_append_start_object.restype = ctypes.c_int

bson_append_finish_object = _.bson_append_finish_object
bson_append_finish_object.argtypes = [BSONREF]
bson_append_finish_object.restype = ctypes.c_int

# Return type in the original bson.h declaration is char *, but it really is
# a data array, so we use c_void_p here to prevent Python casting it to bytes.
# Consumer of this method should use ctypes.string_at to get the content.
bson_data2 = _.bson_data2
bson_data2.argtypes = [BSONREF, ctypes.POINTER(ctypes.c_int)]
bson_data2.restype = ctypes.c_void_p

bson_size2 = _.bson_size2
bson_size2.argtypes = [ctypes.c_void_p]
bson_size2.restype = ctypes.c_int

bson_oid_from_string = _.bson_oid_from_string
bson_oid_from_string.argtypes = [BSONOIDREF, ctypes.c_char_p]
bson_oid_from_string.restype = None

bson_oid_to_string = _.bson_oid_to_string
bson_oid_to_string.argtypes = [BSONOIDREF, ctypes.c_char_p]
bson_oid_to_string.restype = None

bson_iterator_create = _.bson_iterator_create
bson_iterator_create.argtypes = []
bson_iterator_create.restype = BSONITERREF

bson_iterator_dispose = _.bson_iterator_dispose
bson_iterator_dispose.argtypes = [BSONITERREF]
bson_iterator_dispose.restype = None

bson_iterator_init = _.bson_iterator_init
bson_iterator_init.argtypes = [BSONITERREF, BSONREF]
bson_iterator_init.restype = None

bson_iterator_next = _.bson_iterator_next
bson_iterator_next.argtypes = [BSONITERREF]
bson_iterator_next.restype = ctypes.c_int

bson_iterator_key = _.bson_iterator_key
bson_iterator_key.argtypes = [BSONITERREF]
bson_iterator_key.restype = ctypes.c_char_p

bson_iterator_double_raw = _.bson_iterator_double_raw
bson_iterator_double_raw.argtypes = [BSONITERREF]
bson_iterator_double_raw.restype = ctypes.c_double

bson_iterator_int_raw = _.bson_iterator_int_raw
bson_iterator_int_raw.argtypes = [BSONITERREF]
bson_iterator_int_raw.restype = ctypes.c_int

bson_iterator_long_raw = _.bson_iterator_long_raw
bson_iterator_long_raw.argtypes = [BSONITERREF]
bson_iterator_long_raw.restype = ctypes.c_int64

bson_iterator_bool_raw = _.bson_iterator_bool_raw
bson_iterator_bool_raw.argtypes = [BSONITERREF]
bson_iterator_bool_raw.restype = ctypes.c_bool

bson_iterator_oid = _.bson_iterator_oid
bson_iterator_oid.argtypes = [BSONITERREF]
bson_iterator_oid.restype = BSONOIDREF

# Return type in the original bson.h declaration is char *, but it really is
# a data array, so we use c_void_p here to prevent Python casting it to bytes.
# Consumer of this method should use ctypes.string_at to get the content.
bson_iterator_string = _.bson_iterator_string
bson_iterator_string.argtypes = [BSONITERREF]
bson_iterator_string.restype = ctypes.c_void_p

bson_iterator_string_len = _.bson_iterator_string_len
bson_iterator_string_len.argtypes = [BSONITERREF]
bson_iterator_string_len.restype = ctypes.c_int

bson_iterator_bin_len = _.bson_iterator_bin_len
bson_iterator_bin_len.argtypes = [BSONITERREF]
bson_iterator_bin_len.restype = ctypes.c_int

# Return type in the original bson.h declaration is char, but it really is an
# enum value, so we use c_int instead.
bson_iterator_bin_type = _.bson_iterator_bin_type
bson_iterator_bin_type.argtypes = [BSONITERREF]
bson_iterator_bin_type.restype = ctypes.c_int

# Return type in the original bson.h declaration is char *, but it really is
# a data array, so we use c_void_p here to prevent Python casting it to bytes.
# Consumer of this method should use ctypes.string_at to get the content.
bson_iterator_bin_data = _.bson_iterator_bin_data
bson_iterator_bin_data.argtypes = [BSONITERREF]
bson_iterator_bin_data.restype = ctypes.c_void_p

bson_iterator_date = _.bson_iterator_date
bson_iterator_date.argtypes = [BSONITERREF]
bson_iterator_date.restype = ctypes.c_int64

bson_iterator_subiterator = _.bson_iterator_subiterator
bson_iterator_subiterator.argtypes = [BSONITERREF, BSONITERREF]
bson_iterator_subiterator.restype = None


# For debugging.

bson_print_raw = _.bson_print_raw
bson_print_raw.argtypes = [ctypes.c_char_p, ctypes.c_int]
bson_print_raw.restype = None

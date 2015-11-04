#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, unicode_literals
import binascii
import collections
import copy
import ctypes
import datetime
import hashlib
import uuid

import six

from . import c
from .utils import CObjectWrapper, PrettyOrderedDict, coerce_char_p, coerce_str


HASH = type(hashlib.md5())
INT_MAX = 2 ** 31 - 1
LONG_MAX = 2 ** 63 - 1
ID_KEY_NAME = coerce_char_p(c.JDBIDKEYNAME)


class BSONEncodeError(Exception):
    def __init__(self, obj):
        msg = 'Could not encode object {obj}.'.format(obj=repr(obj))
        super(BSONEncodeError, self).__init__(msg)


class BSONDecodeError(Exception):
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Could not decode BSON object.'
        super(BSONDecodeError, self).__init__(msg)


class MD5(six.binary_type):
    """Custom subclass for content of an MD5 binary field.
    """
    def copy(self):
        return copy.copy(self)

    def digest(self):
        return self.copy()

    def hexdigest(self):
        return coerce_str(binascii.hexlify(self))


def _datetime_to_millis(value):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = value - epoch
    millis = int(delta.total_seconds() * 1000)
    return millis


def _bson_encode_element(key, value, into):
    key = coerce_char_p(key)
    if value is None:
        r = c.bson.append_null(into, key)
    elif isinstance(value, six.text_type):
        value = coerce_char_p(value)
        if key == ID_KEY_NAME:
            oid = c.BSONOID.from_string(value)
            r = c.bson.append_oid(into, key, ctypes.byref(oid))
        else:
            r = c.bson.append_string_n(into, key, value, len(value))
    elif isinstance(value, bool):
        r = c.bson.append_bool(into, key, value)
    elif isinstance(value, six.integer_types):
        # Need to be after bool because bool is a subclass of int.
        if value > LONG_MAX:
            raise BSONEncodeError(value)
        elif value > INT_MAX:
            r = c.bson.append_long(into, key, value)
        else:
            r = c.bson.append_int(into, key, value)
    elif isinstance(value, float):
        r = c.bson.append_double(into, key, value)
    elif isinstance(value, datetime.datetime):
        millis = _datetime_to_millis(value)
        r = c.bson.append_date(into, key, millis)
    elif isinstance(value, datetime.date):
        value = datetime.datetime.combine(value, datetime.datetime.min.time())
        millis = _datetime_to_millis(value)
        r = c.bson.append_date(into, key, millis)
    elif isinstance(value, uuid.UUID):
        data = value.bytes
        r = c.bson.append_binary(into, key, c.BSON_BIN_UUID, data, len(data))
    elif isinstance(value, (HASH, MD5,)):
        data = value.digest()
        r = c.bson.append_binary(into, key, c.BSON_BIN_MD5, data, len(data))
    elif isinstance(value, six.binary_type):
        # Need to be after MD5 because MD5 is a subclass of six.binary_type.
        buf = ctypes.create_string_buffer(value, len(value))
        r = c.bson.append_binary(into, key, c.BSON_BIN_BINARY, buf, len(value))
    elif isinstance(value, collections.Mapping):
        r = c.bson.append_start_object(into, key)
        if r != c.BSON_OK:  # pragma: no cover.
            raise BSONEncodeError(value)
        for k in value:
            _bson_encode_element(k, value[k], into)
        r = c.bson.append_finish_object(into)
        if r != c.BSON_OK:  # pragma: no cover.
            raise BSONEncodeError(value)
    elif isinstance(value, collections.Sequence):
        r = c.bson.append_start_array(into, key)
        if r != c.BSON_OK:  # pragma: no cover.
            raise BSONEncodeError(value)
        for i, v in enumerate(value):
            _bson_encode_element(str(i), v, into)
        r = c.bson.append_finish_array(into)
        if r != c.BSON_OK:  # pragma: no cover.
            raise BSONEncodeError(value)
    else:
        # TODO: Implement tolerence mode, insert undefined for objects not
        # encodable. Or maybe use pickle to save the binary?
        r = c.BSON_ERROR
    if r != c.BSON_OK:
        raise BSONEncodeError(value)


def _bson_decode_double(bsiter):
    value = c.bson.iterator_double_raw(bsiter)
    return value


def _bson_decode_int(bsiter):
    value = c.bson.iterator_int_raw(bsiter)
    return value


def _bson_decode_long(bsiter):
    value = c.bson.iterator_long_raw(bsiter)
    return value


def _bson_decode_bool(bsiter):
    value = c.bson.iterator_bool_raw(bsiter)
    return value


def _bson_decode_oid(bsiter):
    oid_ref = c.bson.iterator_oid(bsiter)
    oid_str = six.text_type(oid_ref.contents)
    return oid_str


def _bson_decode_string(bsiter):
    size = c.bson.iterator_string_len(bsiter)
    data_p = c.bson.iterator_string(bsiter)
    s = ctypes.string_at(data_p, size - 1)  # Minus NULL character.
    return coerce_str(s)


def _bson_decode_date(bsiter):
    timestamp = c.bson.iterator_date(bsiter)
    dt = datetime.datetime.utcfromtimestamp(timestamp / 1000)
    return dt


def _bson_decode_array(bsiter):
    subiter = c.bson.iterator_create()
    c.bson.iterator_subiterator(bsiter, subiter)
    arr = _bson_decode_array_contents(subiter)
    c.bson.iterator_dispose(subiter)
    return arr


def _bson_decode_object(bsiter):
    subiter = c.bson.iterator_create()
    c.bson.iterator_subiterator(bsiter, subiter)
    obj = _bson_decode_object_contents(subiter)
    c.bson.iterator_dispose(subiter)
    return obj


def _bson_decode_binary(bsiter):
    subtype = c.bson.iterator_bin_type(bsiter)
    try:
        subdecoder = _BIN_SUBTYPE_DECODERS[subtype]
    except KeyError:    # pragma: no cover
        raise BSONDecodeError(
            'Could not decode binary with key {key} of type {subtype}'.format(
                key=coerce_str(c.bson.iterator_key(bsiter)),
                subtype=_BIN_SUBTYPE_NAMES[subtype],
            )
        )
    size = c.bson.iterator_bin_len(bsiter)
    data_p = c.bson.iterator_bin_data(bsiter)
    data = ctypes.string_at(data_p, size=size)
    return subdecoder(data)


_TYPE_DECODERS = {
    c.BSON_DOUBLE: _bson_decode_double,
    c.BSON_STRING: _bson_decode_string,
    c.BSON_OBJECT: _bson_decode_object,
    c.BSON_ARRAY: _bson_decode_array,
    c.BSON_BINDATA: _bson_decode_binary,
    c.BSON_UNDEFINED: lambda i: None,
    c.BSON_OID: _bson_decode_oid,
    c.BSON_BOOL: _bson_decode_bool,
    c.BSON_DATE: _bson_decode_date,
    c.BSON_NULL: lambda i: None,
    c.BSON_INT: _bson_decode_int,
    c.BSON_LONG: _bson_decode_long,
}

_TYPE_NAMES = [
    'BSON_EOO',
    'BSON_DOUBLE',
    'BSON_STRING',
    'BSON_OBJECT',
    'BSON_ARRAY',
    'BSON_BINDATA',
    'BSON_UNDEFINED',
    'BSON_OID',
    'BSON_BOOL',
    'BSON_DATE',
    'BSON_NULL',
    'BSON_REGEX',
    'BSON_DBREF',
    'BSON_CODE',
    'BSON_SYMBOL',
    'BSON_CODEWSCOPE',
    'BSON_INT',
    'BSON_TIMESTAMP',
    'BSON_LONG',
]

_BIN_SUBTYPE_DECODERS = {
    c.BSON_BIN_BINARY: lambda data: data,
    c.BSON_BIN_UUID: lambda data: uuid.UUID(bytes=data),
    c.BSON_BIN_MD5: MD5,
}

_BIN_SUBTYPE_NAMES = [
    'BSON_BIN_BINARY',
    'BSON_BIN_FUNC',
    'BSON_BIN_BINARY_OLD',
    'BSON_BIN_UUID',
    'BSON_BIN_MD5',
    'BSON_BIN_USER',
]


def _bson_decode_array_contents(subiter):
    subitems = []
    while True:
        value_type = c.bson.iterator_next(subiter)
        if value_type == c.BSON_EOO:
            break
        key = coerce_str(c.bson.iterator_key(subiter))
        try:
            key = int(key)
            assert key == len(subitems)
        except (AssertionError, ValueError):    # pragma: no cover
            # Error if the keys are not integers representing array indexes.
            # This shouldn't happen if the BSON object is valid.
            # TODO: Better error message.
            raise BSONDecodeError
        try:
            decoder = _TYPE_DECODERS[value_type]
        except KeyError:    # pragma: no cover
            raise BSONDecodeError(
                'Could not decode object with key {key} of type {type}'.format(
                    key=key, type=_TYPE_NAMES[value_type],
                )
            )
        subitems.append(decoder(subiter))
    return subitems


def _bson_decode_object_contents(subiter):
    subitems = PrettyOrderedDict()
    while True:
        value_type = c.bson.iterator_next(subiter)
        if value_type == c.BSON_EOO:
            break
        key = coerce_str(c.bson.iterator_key(subiter))
        try:
            decoder = _TYPE_DECODERS[value_type]
        except KeyError:    # pragma: no cover
            raise BSONDecodeError(
                'Could not decode object with key {key} of type {type}'.format(
                    key=key, type=_TYPE_NAMES[value_type],
                )
            )
        subitems[key] = decoder(subiter)
    return subitems


def _get_data(bs):
    sz = ctypes.c_int()
    data_p = c.bson.data2(bs._wrapped, ctypes.byref(sz))
    data = ctypes.string_at(data_p, sz.value)
    return data


class BSON(CObjectWrapper):
    """Wrapper for a BSON construct.
    """
    def __init__(self, wrapped):
        """Initialize a wrapper for a *finished* BSON struct.

        :param wrapped: BSON struct to be wrapped.
        :param managed: Whether the wrapped BSON struct should be deleted on
            object deletion. Defaults to `True`.
        """
        super(BSON, self).__init__(wrapped=wrapped, finalizer=c.bson.del_)

    @classmethod
    def encode(cls, obj, as_query=False):
        """Encode a Python object into BSON.
        """
        if not isinstance(obj, collections.Mapping):
            raise BSONEncodeError(obj)
        wrapped = c.bson.create()
        if as_query:
            c.bson.init_as_query(wrapped)
        else:
            c.bson.init(wrapped)
        for key in obj:
            _bson_encode_element(key=key, value=obj[key], into=wrapped)
        c.bson.finish(wrapped)
        return cls(wrapped)

    def decode(self):
        bsiter = c.bson.iterator_create()
        c.bson.iterator_init(bsiter, self._wrapped)
        obj = _bson_decode_object_contents(bsiter)
        c.bson.iterator_dispose(bsiter)
        return obj

    def __repr__(self):     # pragma: no cover
        return '<BSON {data}>'.format(data=repr(_get_data(self)))

    def __eq__(self, other):
        if self is other:
            return True
        if isinstance(other, BSON):
            other = _get_data(other)
        return _get_data(self) == other

    def __ne__(self, other):    # pragma: no cover
        return not (self == other)


def encode(obj, as_query=False):
    return BSON.encode(obj, as_query)


def decode(bs):
    return bs.decode()

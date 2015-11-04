#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import collections
import datetime
import hashlib
import uuid

import pytest
import six

from ejdb import bson


if six.PY2:
    def test_bson_invalid():
        with pytest.raises(bson.BSONEncodeError) as ctx:
            bson.encode(['msyok'])
        assert str(ctx.value) == "Could not encode object [u'msyok']."

        with pytest.raises(bson.BSONEncodeError) as ctx:
            bson.encode('msyok')
        assert str(ctx.value) == "Could not encode object u'msyok'."
else:
    def test_bson_invalid():
        with pytest.raises(bson.BSONEncodeError) as ctx:
            bson.encode(['msyok'])
        assert str(ctx.value) == "Could not encode object ['msyok']."

        with pytest.raises(bson.BSONEncodeError) as ctx:
            bson.encode('msyok')
        assert str(ctx.value) == "Could not encode object 'msyok'."


def test_bson_decode():
    data = {'answer': '42'}
    bs = bson.encode(data)
    assert bson.decode(bs) == data


def test_bson_eq():
    bs = bson.encode({'answer': '42'})
    assert bs == bs
    assert bs == bson.encode({'answer': '42'})
    assert bs == b'\x14\x00\x00\x00\x02answer\x00\x03\x00\x00\x0042\x00\x00'
    assert not (bs == b'')
    assert not (bs == '')


def test_bson_number():
    bs = bson.encode(collections.OrderedDict([
        ('int', 42),
        ('long', 2 ** 40),
        ('double', 9.2),
    ]))
    assert (
        bs ==
        b',\x00\x00\x00\x10int\x00*\x00\x00\x00\x12long\x00\x00\x00\x00\x00'
        b'\x00\x01\x00\x00\x01double\x00ffffff"@\x00'
    )
    assert bs.decode() == {
        'int': 42,
        'long': 2 ** 40,
        'double': 9.2,
    }


if six.PY2:
    def test_bson_int_too_large():
        with pytest.raises(bson.BSONEncodeError) as ctx:
            bson.encode({'too-large': 2 ** 100})
        assert (str(ctx.value) ==
                'Could not encode object 1267650600228229401496703205376L.')
else:
    def test_bson_int_too_large():
        with pytest.raises(bson.BSONEncodeError) as ctx:
            bson.encode({'too-large': 2 ** 100})
        assert (str(ctx.value) ==
                'Could not encode object 1267650600228229401496703205376.')


def test_bson_null():
    bs = bson.encode({'give up': None})
    assert bs == b'\x0e\x00\x00\x00\ngive up\x00\x00'
    assert bs.decode() == {'give up': None}


def test_bson_bool():
    bs = bson.encode({'always': True})
    assert bs == b'\x0e\x00\x00\x00\x08always\x00\x01\x00'
    assert bs.decode() == {'always': True}


def test_bson_oid():
    bs = bson.encode({'_id': '0123456789abcdef01234567'})
    assert (
        bs == b'\x16\x00\x00\x00\x07_id\x00\x01#Eg\x89\xab\xcd\xef\x01#Eg\x00')
    assert bs.decode() == {'_id': '0123456789abcdef01234567'}


def test_bson_string():
    bs = bson.encode({'answer': '42'})
    assert bs == b'\x14\x00\x00\x00\x02answer\x00\x03\x00\x00\x0042\x00\x00'
    assert bs.decode() == {'answer': '42'}


def test_bson_binary():
    bs = bson.encode({'<3': b'\xe2\x99\xa5'})
    assert (
        bs ==
        b'\x11\x00\x00\x00\x05<3\x00\x03\x00\x00\x00\x00\xe2\x99\xa5\x00'
    )
    assert bs.decode() == {'<3': b'\xe2\x99\xa5'}


def test_bson_uuid():
    bs = bson.encode({'uuid': uuid.UUID('12345678123456781234567812345678')})
    assert (
        bs ==
        b' \x00\x00\x00\x05uuid\x00\x10\x00\x00\x00\x03\x124Vx\x124Vx\x124Vx'
        b'\x124Vx\x00'
    )
    assert (
        bs.decode() == {'uuid': uuid.UUID('12345678123456781234567812345678')})


def test_bson_md5():
    m = hashlib.md5(b'msyok')
    bs = bson.encode({'md5': m})
    assert (
        bs ==
        b'\x1f\x00\x00\x00\x05md5\x00\x10\x00\x00\x00\x05\x80\xbf\x9c\xff\x7f'
        b'\\\xc3\xf2\x18n\x9d\xc8\xf0\xc3\xd5\xf3\x00'
    )
    obj = bs.decode()
    assert list(obj.keys()) == ['md5']
    assert obj['md5'].digest() == m.digest()


def test_bson_md5_custom():
    hashm = hashlib.md5(b'msyok')
    m = bson.MD5(hashm.digest())
    bs = bson.encode({'md5': m})
    assert (
        bs ==
        b'\x1f\x00\x00\x00\x05md5\x00\x10\x00\x00\x00\x05\x80\xbf\x9c\xff\x7f'
        b'\\\xc3\xf2\x18n\x9d\xc8\xf0\xc3\xd5\xf3\x00'
    )
    obj = bs.decode()
    assert obj == {'md5': m}
    assert obj['md5'].hexdigest() == hashm.hexdigest()


def test_bson_date():
    bs = bson.encode({'date': datetime.date(2015, 8, 19)})
    assert bs == b'\x13\x00\x00\x00\tdate\x00\x00XACO\x01\x00\x00\x00'
    assert bs.decode() == {'date': datetime.datetime(2015, 8, 19)}


def test_bson_datetime():
    bs = bson.encode({'date': datetime.datetime(2015, 8, 19)})
    assert bs == b'\x13\x00\x00\x00\tdate\x00\x00XACO\x01\x00\x00\x00'
    assert bs.decode() == {'date': datetime.datetime(2015, 8, 19)}


def test_bson_array_object():
    bs = bson.encode({
        'flowers': ['red', 'blue'],
        'others': [
            {'sugar': 'sweet'},
            {'so are': 'you'},
        ]
    })
    assert bs.decode() == {
        'flowers': ['red', 'blue'],
        'others': [
            {'sugar': 'sweet'},
            {'so are': 'you'},
        ]
    }


def test_bson_unrecognized():

    class Thing(object):
        def __repr__(self):
            return 'Thing'

    with pytest.raises(bson.BSONEncodeError) as ctx:
        bson.encode({'thing': Thing()})
    assert str(ctx.value) == 'Could not encode object Thing.'


def test_bson_decode_error_message():
    with pytest.raises(bson.BSONDecodeError) as ctx:
        raise bson.BSONDecodeError()
    assert str(ctx.value) == 'Could not decode BSON object.'

    with pytest.raises(bson.BSONDecodeError) as ctx:
        raise bson.BSONDecodeError('msyok')
    assert str(ctx.value) == 'msyok'

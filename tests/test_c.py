#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import pytest

from ejdb import c


def test_bsonoid():
    oid = c.BSONOID.from_string('0123456789abcdef01234567')
    assert str(oid) == '0123456789abcdef01234567'


def test_bsonoid_too_short():
    with pytest.raises(ValueError) as ctx:
        c.BSONOID.from_string('123456789abcdef01234567')
    assert str(ctx.value) == 'OID should be a 24-character-long hex string.'


def test_bsonoid_not_hex():
    with pytest.raises(ValueError) as ctx:
        c.BSONOID.from_string('123456789abcdefg01234567')
    assert str(ctx.value) == 'OID should be a 24-character-long hex string.'


def test_bsonoid_too_long():
    with pytest.raises(ValueError) as ctx:
        c.BSONOID.from_string('0123456789abcdef012345678')
    assert str(ctx.value) == 'OID should be a 24-character-long hex string.'

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import os
import re
import shutil
import tempfile

from ctypes import byref

import pytest
import six

from ejdb import api, c


def test_get_ejdb_version():
    assert api.get_ejdb_version() == c.ejdb.version().decode('utf-8')


def test_is_valid_oid():
    assert api.is_valid_oid('0123456789abcdef01234567')         # OK.
    assert not api.is_valid_oid('0123456789abcdef012345678')    # Too long.
    assert not api.is_valid_oid('0123456789abcdefg123456')      # Not hex.
    assert not api.is_valid_oid('0123456789abcdef123456')       # Too short.


class TestDatabaseInvalid(object):

    def test_init_defaultpath(self):
        """An EJDB constructed with default args does not have a path, and is
        not opened.
        """
        jb = api.Database()
        assert not jb.is_open()
        assert not jb.path
        assert jb.options == api.READ

    def test_init_nopath(self):
        """An EJDB with a path evaluated to `False` is not opened.
        """
        jb = api.Database(path='')
        assert not jb.is_open()

    def test_init_notexist(self):
        """If a non-existant path is given, constructing raises an error.
        """
        tfile = tempfile.NamedTemporaryFile()
        with pytest.raises(api.DatabaseError):
            api.Database(path=tfile.name)

    def test_init_notexist_autocreate(self):
        """Construction is automatic if we give the `CREATE` flag.
        """
        dirpath = tempfile.mkdtemp()
        path = os.path.join(dirpath, 'yksom')
        assert not os.path.exists(path)
        jb = api.Database(path=path, options=(api.WRITE | api.CREATE))
        jb.close()
        shutil.rmtree(dirpath)

    def test_path_setter(self):
        """An unopend database can change its path.
        """
        jb = api.Database(path='')
        assert jb.path == ''
        jb.path = 'wawawa'
        assert jb.path == 'wawawa'

    def test_option_setter(self):
        """An unopend database can change its options.
        """
        jb = api.Database(options=0)
        assert jb.options == 0
        jb.options = 17
        assert jb.options == 17

    def test_open_nopath(self):
        """A database without a path cannot be opened.
        """
        jb = api.Database(path='')
        with pytest.raises(api.DatabaseError) as ctx:
            jb.open()
        assert str(ctx.value) == 'File not found.'


class TestDatabaseInit(object):

    def setup(self):
        """Setup a temp database for read-only tests.
        """
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'yksom')
        assert not os.path.exists(path)
        self.path = path
        if not isinstance(path, six.binary_type):
            path = path.encode('utf-8')
        self.path_b = path

        # Create an empty database.
        jb = c.ejdb.new()
        c.ejdb.open(jb, self.path_b, c.JBOWRITER | c.JBOCREAT)
        c.ejdb.close(jb)
        c.ejdb.del_(jb)

    def teardown(self):
        if self.jb.is_open():
            self.jb.close()
        shutil.rmtree(self.dirpath)

    def test_init(self):
        self.jb = api.Database(path=self.path)
        assert self.jb.is_open()

    def test_init_bytespath(self):
        self.jb = api.Database(path=self.path_b)
        assert self.jb.is_open()

    def test_open_reopen(self):
        self.jb = api.Database(path=self.path)
        with pytest.raises(api.DatabaseError) as ctx:
            self.jb.open()
        assert str(ctx.value) == 'Database already opened.'

        self.jb.close()
        assert not self.jb.is_open()

        self.jb.open()
        assert self.jb.is_open()

    def test_close_reclose(self):
        self.jb = api.Database(path=self.path)
        self.jb.close()
        with pytest.raises(api.DatabaseError) as ctx:
            self.jb.close()
        assert str(ctx.value) == 'Database not opened.'

    def test_path_setter_opened(self):
        """An open database cannot change its path.
        """
        self.jb = api.Database(path=self.path)
        assert isinstance(self.jb.path, six.string_types)
        assert self.jb.path == self.path
        with pytest.raises(api.DatabaseError) as ctx:
            self.jb.path = 'yksom'
        assert str(ctx.value) == 'Could not set path to an open database.'

    def test_option_setter_opened(self):
        self.jb = api.Database(
            path=self.path, options=(api.WRITE | api.TRUNCATE),
        )
        assert self.jb.options == (api.WRITE | api.TRUNCATE)
        with pytest.raises(api.DatabaseError) as ctx:
            self.jb.options = api.READ
        assert str(ctx.value) == 'Could not set options to an open database.'


class TestDatabase(object):

    def setup(self):
        """Create a database for testing.
        """
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'yksom')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )

    def teardown(self):
        if self.jb.is_open():
            self.jb.close()
        shutil.rmtree(self.dirpath)

    if six.PY2:
        def test_get_collection(self):
            c.ejdb.createcoll(self.jb._wrapped, b'yksom', byref(c.EJCOLLOPTS()))
            assert self.jb.get_collection('yksom')
            with pytest.raises(api.CollectionDoesNotExist) as ctx:
                self.jb.get_collection('yksomevoli')
            assert str(ctx.value) == "u'yksomevoli'"

        def test_getitem(self):
            c.ejdb.createcoll(self.jb._wrapped, b'yksom', byref(c.EJCOLLOPTS()))
            assert self.jb.get_collection('yksom')
            with pytest.raises(KeyError) as ctx:
                self.jb['yksomevoli']
            assert str(ctx.value) == "u'yksomevoli'"
    else:
        def test_get_collection(self):
            c.ejdb.createcoll(self.jb._wrapped, b'yksom', byref(c.EJCOLLOPTS()))
            assert self.jb.get_collection('yksom')
            with pytest.raises(api.CollectionDoesNotExist) as ctx:
                self.jb.get_collection('yksomevoli')
            assert str(ctx.value) == "'yksomevoli'"

        def test_getitem(self):
            c.ejdb.createcoll(self.jb._wrapped, b'yksom', byref(c.EJCOLLOPTS()))
            assert self.jb.get_collection('yksom')
            with pytest.raises(KeyError) as ctx:
                self.jb['yksomevoli']
            assert str(ctx.value) == "'yksomevoli'"

    def test_create_collection(self):
        assert not c.ejdb.getcoll(self.jb._wrapped, b'yksom')
        self.jb.create_collection('yksom')
        assert c.ejdb.getcoll(self.jb._wrapped, b'yksom')

        with pytest.raises(api.DatabaseError) as ctx:
            self.jb.create_collection('yksom')
        assert str(ctx.value) == "Collection with name 'yksom' already exists."

        self.jb.create_collection('yksom', exist_ok=True)

    def test_has_collection(self):
        assert self.jb.has_collection('yksom') is False
        self.jb.create_collection('yksom')
        assert self.jb.has_collection('yksom') is True

    def test_contains(self):
        assert 'yksom' not in self.jb
        self.jb.create_collection('yksom')
        assert 'yksom' in self.jb

    def test_drop_collection(self):
        self.jb.create_collection('yksom')
        assert 'yksom' in self.jb
        self.jb.drop_collection('yksom')
        assert 'yksom' not in self.jb

        # Dropping a non-existent collection is NOOP.
        self.jb.drop_collection('yksom')


class TestCollection(object):

    def setup(self):
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'yksom')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )

        self.jb.create_collection('yksom')
        self.jb.create_collection('evolyksom')
        self.jb.create_collection('yksomevoli')

    def teardown(self):
        if self.jb.is_open():
            self.jb.close()
        shutil.rmtree(self.dirpath)

    def test_name(self):
        assert self.jb['yksom'].name == 'yksom'

    def test_database_iter(self):
        i = 0
        for collection in self.jb:
            i += 1
        assert i == 3

    def test_database_len(self):
        assert len(self.jb) == 3

    def test_save(self):
        coll = self.jb['yksom']
        document = {'category': '♥', 'name': 'Mosky'}
        coll.save(document)
        assert re.match(r'^[0-9a-fA-F]{24}$', document['_id']) is not None
        # TODO: Check the collection content (using only C API).

    def test_insert_many(self):
        # TODO: Implement me.
        pass


class TestCollectionRetrieval(object):

    def setup(self):
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'yksom')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )
        self.jb.create_collection('yksom')
        self.coll = self.jb['yksom']

        self.objs = [
            {'order': 4, 'one': 1},
            {'order': 5, 'two': 2},
            {'order': 3, 'three': 3},
            {'order': 0, 'four': 4},
            {'order': -1, 'five': 5},
        ]
        oids = self.coll.insert_many(self.objs)
        for oid, obj in zip(oids, self.objs):
            obj['_id'] = oid

    def teardown(self):
        if self.jb.is_open():
            self.jb.close()
        shutil.rmtree(self.dirpath)

    def test_eq(self):
        cur = self.coll.find()
        assert cur == cur
        assert cur == self.coll.find()
        assert cur != self.coll.find({'one': 1})

    def test_find_all(self):
        for i, obj in enumerate(self.coll.find()):
            assert dict(obj) == self.objs[i]

    def test_find_with_query(self):
        objs = self.coll.find({'one': 1})
        assert len(objs) == 1
        assert dict(objs[0]) == self.objs[0]

    def test_find_with_hints(self):
        objs = self.coll.find(hints={'$orderby': {'order': 1}})
        assert len(objs) == 5
        assert dict(objs[0]) == self.objs[4]
        assert dict(objs[1]) == self.objs[3]
        assert dict(objs[2]) == self.objs[2]
        assert dict(objs[3]) == self.objs[0]
        assert dict(objs[4]) == self.objs[1]

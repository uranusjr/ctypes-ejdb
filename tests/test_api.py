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
        path = os.path.join(dirpath, 'msyok')
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

    def test_writable(self):
        jb = api.Database(path='')
        assert not jb.writable

    def test_not_writable(self):
        jb = api.Database(path='', options=api.WRITE)
        assert jb.writable


class TestDatabaseInit(object):

    def setup(self):
        """Setup a temp database for read-only tests.
        """
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'msyok')
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
            self.jb.path = 'msyok'
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
        path = os.path.join(self.dirpath, 'msyok')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )

    def teardown(self):
        if self.jb.is_open():
            self.jb.close()
        shutil.rmtree(self.dirpath)

    if six.PY2:
        def test_get_collection(self):
            c.ejdb.createcoll(
                self.jb._wrapped, b'msyok', byref(c.EJCOLLOPTS()),
            )
            assert self.jb.get_collection('msyok')
            with pytest.raises(api.CollectionDoesNotExist) as ctx:
                self.jb.get_collection('msyokevoli')
            assert str(ctx.value) == "u'msyokevoli'"

        def test_getitem(self):
            c.ejdb.createcoll(
                self.jb._wrapped, b'msyok', byref(c.EJCOLLOPTS()),
            )
            assert self.jb.get_collection('msyok')
            with pytest.raises(KeyError) as ctx:
                self.jb['msyokevoli']
            assert str(ctx.value) == "u'msyokevoli'"
    else:
        def test_get_collection(self):
            c.ejdb.createcoll(
                self.jb._wrapped, b'msyok', byref(c.EJCOLLOPTS()),
            )
            assert self.jb.get_collection('msyok')
            with pytest.raises(api.CollectionDoesNotExist) as ctx:
                self.jb.get_collection('msyokevoli')
            assert str(ctx.value) == "'msyokevoli'"

        def test_getitem(self):
            c.ejdb.createcoll(
                self.jb._wrapped, b'msyok', byref(c.EJCOLLOPTS()),
            )
            assert self.jb.get_collection('msyok')
            with pytest.raises(KeyError) as ctx:
                self.jb['msyokevoli']
            assert str(ctx.value) == "'msyokevoli'"

    def test_create_collection(self):
        assert not c.ejdb.getcoll(self.jb._wrapped, b'msyok')
        self.jb.create_collection('msyok')
        assert c.ejdb.getcoll(self.jb._wrapped, b'msyok')

        with pytest.raises(api.DatabaseError) as ctx:
            self.jb.create_collection('msyok')
        assert str(ctx.value) == "Collection with name 'msyok' already exists."

        self.jb.create_collection('msyok', exist_ok=True)

    def test_has_collection(self):
        assert self.jb.has_collection('msyok') is False
        self.jb.create_collection('msyok')
        assert self.jb.has_collection('msyok') is True

    def test_contains(self):
        assert 'msyok' not in self.jb
        self.jb.create_collection('msyok')
        assert 'msyok' in self.jb

    def test_drop_collection(self):
        self.jb.create_collection('msyok')
        assert 'msyok' in self.jb
        self.jb.drop_collection('msyok')
        assert 'msyok' not in self.jb

        # Dropping a non-existent collection is NOOP.
        self.jb.drop_collection('msyok')

    def test_collections(self):
        self.jb.create_collection('msyok')
        colls = self.jb.collections
        assert len(colls) == 1
        assert colls.pop().name == 'msyok'

    def test_collection_names(self):
        coll = self.jb.create_collection('msyok')
        assert self.jb.collection_names == {coll.name}


class TestCollection(object):

    def setup(self):
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'msyok')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )

        self.jb.create_collection('msyok')
        self.jb.create_collection('evolmsyok')
        self.jb.create_collection('msyokevoli')

    def teardown(self):
        if self.jb.is_open():
            self.jb.close()
        shutil.rmtree(self.dirpath)

    def test_name(self):
        assert self.jb['msyok'].name == 'msyok'

    def test_database_iter(self):
        i = 0
        for collection in self.jb:
            i += 1
        assert i == 3

    def test_database_len(self):
        assert len(self.jb) == 3

    def test_save(self):
        coll = self.jb['msyok']
        document = {'category': '♥', 'name': 'Mosky'}
        coll.save(document)
        assert re.match(r'^[0-9a-fA-F]{24}$', document['_id']) is not None
        # TODO: Check the collection content (using only C API).


class TestCollectionRetrieval(object):

    def setup(self):
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'msyok')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )
        self.jb.create_collection('msyok')
        self.coll = self.jb['msyok']

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

    def test_count_all(self):
        assert self.coll.count() == 5

    def test_count_with_query(self):
        assert self.coll.count({'one': 1}) == 1

    def test_find_all(self):
        objs = self.coll.find()
        assert len(objs) == 5
        for i, obj in enumerate(objs):
            assert dict(obj) == self.objs[i]
        assert objs == self.objs

    def test_find_all_invalid(self):
        with pytest.raises(api.CommandError):
            self.coll.find({'one': 1, '$bobo': None})

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


class TestCollectionDeletion(object):

    def setup(self):
        self.dirpath = tempfile.mkdtemp()
        path = os.path.join(self.dirpath, 'msyok')
        self.jb = api.Database(
            path=path, options=(api.WRITE | api.TRUNCATE | api.CREATE),
        )
        self.jb.create_collection('msyok')
        self.coll = self.jb['msyok']

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

    def test_delete_one(self):
        result_count = self.coll.delete_one({'one': 1})
        assert result_count == 1
        assert self.coll.find() == self.objs[1:]

    def test_delete_many(self):
        result_count = self.coll.delete_many({'one': 1})
        assert result_count == 1
        assert self.coll.find() == self.objs[1:]

    def test_delete_many_empty(self):
        result_count = self.coll.delete_many()
        assert result_count == 5
        assert self.coll.find() == []

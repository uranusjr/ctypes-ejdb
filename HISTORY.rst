.. :changelog:

History
=======

0.4.7 (2016-07-20)
---------------------

* Fix crash when querying with invalid parameter names. This now raises an
  ``CommandError``.
* Fix memory leak when calling ``Collection.count``.
* Add API to query for a list of collection names in a database without needing
  to construct the collections themselves.
* Add API to check whether a database is writable.
* Add flag to disable coloring in CLI, and disable it on Windows by default.
* ``ejdb.cli`` now has a ``--version`` option.


0.4.6 (2015-10-06)
---------------------

* Fix Python 2 compatibility regarding `ejdb.cfg` usage.
* Fix segmentation fault when trying to reuse collection instances retrieved
  from iterating through a database.
* ``ejdb.cli`` now creates a non-existent database if the path given does not
  exist.
* Add a more meaningful error message when the EJDB binary path is not
  configured properly.
* Fix documentation on `Collection.delete_one()` and
  `Collection.delete_many()`.


0.4.5 (2015-09-07)
---------------------

* Fix ``Collection.delete_one`` and ``Collection.delete_many``.


0.4.4 (2015-07-30)
---------------------

* Fix query flag passing.


0.4.3 (2015-07-29)
---------------------

* Move ``exit()`` fix in CLI.


0.4.2 (2015-07-29)
---------------------

* Fix ``exit()`` call in CLI.


0.4.1 (2015-07-27)
---------------------

* Fix missing ``NOBLOCK`` constant.


0.4 (2015-07-25)
---------------------

* Move command line interface dependencies to extras. New installations now needs to run ``pip install ctypes-ejdb[cli]`` to install it. This is better for those who want only the core library.


0.3.3 (2015-07-24)
---------------------

* Fix Python 2 compatibility.


0.3.2 (2015-07-07)
---------------------

* Fix attribute lookup in ``DatabaseError`` construction.
* Add options to config EJDB path by environ or ``.cfg`` file.
* Make document repr look like a dict so it prints better.


0.3.1 (2015-07-03)
---------------------

* Fixed context manager usage opening a ``Database``.
* Fixed attribute error in ``Collection.count``.
* Fixed document iterator slicing.
* Experimental CLI utility ``ejdb.cli`` based on Click and ptpython.


0.3 (2015-07-01)
---------------------

* Make EJDB path configurable with ``ejdb.init(path)``.


0.2.1 (2015-07-01)
---------------------

* Add save shortcut on database.


0.2 (2015-07-01)
---------------------

* Fix segmentation fault when converting BSON OID to string.
* Fix error message retrieval in ``Database.close``.
* Tests now run on Windows.


0.1.1 (2015-06-30)
---------------------

* Fix encoding error in ``pip install``.


0.1.0 (2015-06-28)
---------------------

* First release on PyPI.

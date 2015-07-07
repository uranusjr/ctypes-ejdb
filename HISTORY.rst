.. :changelog:

History
=======

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

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals
import pprint
import traceback

import click
import ejdb
import six
import standardpaths

from prompt_toolkit.history import FileHistory
from prompt_toolkit.shortcuts import get_input
from ptpython.completer import PythonCompleter

if six.PY3:
    from pygments.lexers.python import Python3Lexer as PythonLexer
else:
    from pygments.lexers.python import PythonLexer


standardpaths.configure(application_name='ctypes-ejdb')


def print_exc(chain=True):
    if six.PY3:
        traceback.print_exc(chain=chain)
    else:
        traceback.print_exc()


@click.command()
@click.argument('db', type=click.Path(exists=True), metavar='path_to_database')
@click.option(
    'lib', '--ejdb', type=click.Path(exists=True), default=None,
    help='(Optional) Path to EJDB C library.',
)
def main(db, lib):
    """Open a database to manipulate on.

    The opened database will be assigned to `db` to be used in the REPL shell.
    """
    if lib is not None:
        ejdb.init(lib)

    data_path = standardpaths.get_writable_path('app_local_data')
    if not data_path.exists():
        data_path.mkdir(parents=True)

    db = ejdb.Database(path=db, options=(ejdb.WRITE | ejdb.CREATE))

    history = FileHistory(str(data_path / 'history'))
    while True:
        try:
            inp = get_input(
                '>>> ',
                completer=PythonCompleter(
                    get_locals=locals, get_globals=globals,
                ),
                history=history, lexer=PythonLexer,
            )
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        result = None
        try:
            result = eval(inp, globals(), locals())
        except SyntaxError:
            try:
                six.exec_(inp, globals(), locals())
            except:
                print_exc(chain=False)
        except:
            print_exc(chain=False)

        if result is None:
            pass
        # HACK: Eval iterators automatically so that find() calls and others
        # return the result without iterating manually.
        # TODO: Find a better solution for this.
        elif (six.PY3 and hasattr(result, '__next__') or
                six.PY2 and hasattr(result, 'next')):
            pprint.pprint([x for x in result])
        else:
            pprint.pprint(result)

    db.close()
    print('Bye!')

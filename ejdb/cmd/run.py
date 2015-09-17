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
from pygments import highlight
from pygments.formatters import Terminal256Formatter

if six.PY3:
    from pygments.lexers.python import (
        Python3Lexer as PythonLexer,
        Python3TracebackLexer as PythonTracebackLexer,
    )
else:
    from pygments.lexers.python import PythonLexer, PythonTracebackLexer


standardpaths.configure(application_name='ctypes-ejdb')


def print_exc(chain=True):
    if six.PY3:
        rep = traceback.format_exc(chain=chain)
    else:
        rep = traceback.format_exc()
    out = highlight(rep, PythonTracebackLexer(), Terminal256Formatter())
    print(out)


def output(thing):
    rep = pprint.pformat(thing)
    out = highlight(rep, PythonLexer(), Terminal256Formatter())
    print(out)


def run_repl_loop(db, data_path):
    history = FileHistory(str(data_path / 'history'))
    glos = {}
    locs = {'db': db}

    def get_locals():
        return locs

    def get_globals():
        return glos

    while True:
        try:
            inp = get_input(
                '>>> ',
                completer=PythonCompleter(
                    get_locals=get_locals, get_globals=get_globals,
                ),
                history=history, lexer=PythonLexer,
            )
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        result = None
        try:
            result = eval(inp, glos, locs)
        except SyntaxError:
            try:
                six.exec_(inp, glos, locs)
            except:
                print_exc(chain=False)
        except SystemExit:
            break
        except:
            print_exc(chain=False)

        if result is None:
            pass
        # HACK: Eval iterators automatically so that find() calls and others
        # return the result without iterating manually.
        # TODO: Find a better solution for this.
        elif (six.PY3 and hasattr(result, '__next__') or
                six.PY2 and hasattr(result, 'next')):
            output([x for x in result])
        else:
            output(result)


@click.command()
@click.argument('db', type=click.Path(), metavar='path_to_database')
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

    with ejdb.Database(path=db, options=(ejdb.WRITE | ejdb.CREATE)) as db:
        run_repl_loop(db, data_path)

    print('Bye!')

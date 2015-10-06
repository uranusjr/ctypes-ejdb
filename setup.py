#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def get_version():
    code = None
    path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'ejdb',
        '__init__.py',
    )
    with open(path) as f:
        for line in f:
            if line.startswith('VERSION'):
                code = line[len('VERSION = '):]
    return '.'.join([str(c) for c in eval(code)])


readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

requirements = [
    'six',
]

extras_requirements = {
    'cli': [
        'click',
        'ptpython',
        'pystandardpaths',
    ],
}

test_requirements = [
    'pytest',
    'pytest-cov',
]

setup(
    name='ctypes-ejdb',
    version=get_version(),
    description='Python binding for EJDB built on ctypes.',
    long_description=readme + '\n\n' + history,
    author='Tzu-ping Chung',
    author_email='uranusjr@gmail.com',
    url='https://github.com/uranusjr/ctypes-ejdb',
    packages=['ejdb', 'ejdb/cmd'],
    entry_points={
        'console_scripts': [
            'ejdb.cli = ejdb.cmd:main',
        ],
    },
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_requirements,
    license='BSD',
    zip_safe=False,
    keywords='ctypes-ejdb',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
)

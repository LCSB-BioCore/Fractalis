# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

setup(
    name = 'fractalis',
    packages = find_packages(),
    install_requires = [
        'Flask',
        'celery'
    ],
    tests_require = [
        'test'
    ],
    test_suite = 'tests',
)

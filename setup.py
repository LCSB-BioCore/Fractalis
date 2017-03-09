# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

setup(
    name='fractalis',
    packages=find_packages(),
    install_requires=[
        'Flask',
        'jsonschema',
        'celery[redis]',
        'redis',
        'pandas',
        'numpy',
        'requests'
    ],
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest==3.0.3',
        'pytest-cov',
        'pytest-mock',
    ]
)

# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

setup(
    name='fractalis',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Flask',
        'flask-cors',
        'Flask-Script',
        'flask-request-id-middleware',
        'jsonschema',
        'celery[redis]',
        'redis',
        'pandas',
        'numpy',
        'scipy',
        'requests',
        'PyYAML',
        'pycrypto',
        'pycryptodomex',
        'rpy2'
    ],
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest==3.0.3',
        'pytest-cov',
        'pytest-mock',
        'responses'
    ]
)

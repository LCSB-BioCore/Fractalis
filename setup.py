# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

setup(
    name='fractalis',
    packages=find_packages(),
    author='Sascha Herzinger',
    author_email='sascha.herzinger@uni.lu',
    url='https://git-r3lab.uni.lu/Fractalis/fractalis',
    version='0.5.2',
    license='Apache2.0',
    include_package_data=True,
    python_requires='>=3.4.0',
    install_requires=[
        'Flask',
        'flask-cors',
        'Flask-Script',
        'flask-request-id-middleware',
        'flask-compress',
        'typing',
        'jsonschema',
        'celery[redis]',
        'redis',
        'numpy',
        'scipy',
        'pandas',
        'sklearn',
        'requests',
        'PyYAML',
        'pycryptodomex',
        'rpy2',
        'flake8',
        'pytest',
        'pytest-runner',
        'pytest-cov',
        'responses',
        'twine'
    ]
)

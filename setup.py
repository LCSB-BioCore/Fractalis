# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

setup(
    name='fractalis',
    packages=find_packages(),
    author='Sascha Herzinger',
    author_email='sascha.herzinger@uni.lu',
    url='https://git-r3lab.uni.lu/Fractalis/fractalis',
    version='0.1.4',
    license='Apache2',
    include_package_data=True,
    python_requires='>=3.4',
    install_requires=[
        'Flask==0.12.2',
        'flask-cors==3.0.3',
        'Flask-Script==2.0.6',
        'flask-request-id-middleware==1.1',
        'flask-compress==1.4.0',
        'typing==3.6.2',
        'jsonschema==2.6.0',
        'celery[redis]==4.1.0',
        'redis==2.10.6',
        'numpy==1.13.3',
        'scipy==0.19.1',
        'pandas==0.20.3',
        'sklearn==0.0',
        'requests==2.18.4',
        'PyYAML==3.12',
        'pycryptodomex==3.4.7',
        'rpy2==2.9.0'
    ]
)

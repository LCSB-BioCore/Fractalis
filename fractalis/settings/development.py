from .base import *

DEBUG = True

INSTALLED_APPS += [
    'django_nose',
]

TEST_RUNNER = 'django_nose.NoseTestSuiteRunner'
NOSE_ARGS = [
    '--with-coverage',
    '--cover-package=data,analytics'
]

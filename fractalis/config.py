"""This module manages the configuration of the Fractalis flask app.

Exports:
    - configure_app -- Function that configures given Flask app
"""
import os

from redislite import Redis


class BaseConfig(object):
    """Basic configuration that should be used in production."""
    DEBUG = False
    TESTING = False
    REDIS_DB_PATH = os.path.join(os.sep, 'tmp', 'fractalis.db')
    rdb = Redis(REDIS_DB_PATH)
    CELERY_BROKER_URL = 'redis+socket://{}'.format(rdb.socket_file)
    CELERY_RESULT_BACKEND = 'redis+socket://{}'.format(rdb.socket_file)


class DevelopmentConfig(BaseConfig):
    """Configuration used in development."""
    DEBUG = True
    TESTING = False


class TestingConfig(BaseConfig):
    """Configuration used in testing."""
    DEBUG = False
    TESTING = True


config = {
    'development': 'fractalis.config.DevelopmentConfig',
    'testing': 'fractalis.config.TestingConfig',
    'production': 'fractalis.config.BaseConfig'
}


def configure_app(app, mode=None):
    """Apply configuration to given flask app based on environment variable.

    This function assumes that the environment variable FRACTALIS_MODE contains
    the key 'development', 'testing', 'production', or is unset in which case
    it defaults to 'production'. Each of these keys maps to a class in this
    module that contains appropriate settings.

    Keyword Arguments:
    app (Flask) -- An instance of the app to configure
    mode (string) -- (optional) Use this instead of the environment variable

    Exceptions:
    KeyError (Exception) -- Is raised when FRACTALIS_MODE contains unknown key
    """
    if mode is None:
        mode = os.getenv('FRACTALIS_MODE', default='production')

    try:
        app.config.from_object(config[mode])
    except KeyError as e:
        raise KeyError("'{}' is no valid value for the FRACTALIS_MODE "
                       "environment variable.".format(mode)) from e

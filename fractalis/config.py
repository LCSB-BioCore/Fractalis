"""This module manages the configuration of the Fractalis flask app.

Exports:
    - configure_app -- Function that configures given Flask app
"""
import os


class BaseConfig(object):
    """Basic configuration that should be used in production."""
    DEBUG = False
    TESTING = False
    CELERY_BROKER_URL = 'redis://localhost:6379'
    CELERY_RESULT_BACKEND = 'redis://localhost:6379'


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


def configure_app(app):
    """Apply configuration to given flask app based on environment variable.

    This function assumes that the environment variable FRACTALIS_MODE contains
    the key 'development', 'testing', 'production', or is unset in which case
    it defaults to 'production'. Each of these keys maps to a class in this
    module that contains appropriate settings.

    Arguments:
    app (Flask) -- An instance of the app to configure

    Exceptions:
    KeyError (Exception) -- Is raised when FRACTALIS_MODE contains unknown key
    """
    mode = os.getenv('FRACTALIS_MODE', default='production')
    try:
        app.config.from_object(config[mode])
    except KeyError as e:
        raise KeyError("'{}' is no valid value for the FRACTALIS_MODE "
                       "environment variable.".format(mode)) from e

import os

class BaseConfig():
    DEBUG = False
    TESTING = False

class DevelopmentConfig(BaseConfig):
    DEBUG = True
    TESTING = False

class TestingConfig(BaseConfig):
    DEBUG = False
    TESTING = True

config = {
    'development': 'fractalis.config.DevelopmentConfig',
    'testing': 'fractalis.config.TestingConfig',
    'production': 'fractalis.config.BaseConfig'
}

def configure_app(app):
    mode = os.getenv('FRACTALIS_MODE', default='production')
    app.config.from_object(config[mode])

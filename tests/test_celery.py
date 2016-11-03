import pytest
from celery import Celery

from fractalis import celery


class TestCelery(object):

    @pytest.fixture
    def flask_app(self):
        from flask import Flask
        app = Flask('test_app')
        app.config['TESTING'] = True
        app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379'
        app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379'
        return app

    def test_exception_if_no_connection_to_broker(self, flask_app):
        flask_app.config['CELERY_BROKER_URL'] = 'foobar'
        with pytest.raises(ConnectionRefusedError):
            celery.init_celery(flask_app)

    def test_exception_if_no_connection_to_result_backend(self, flask_app):
        flask_app.config['CELERY_RESULT_BACKEND'] = 'foobar'
        with pytest.raises(ConnectionRefusedError):
            celery.init_celery(flask_app)

    def test_returns_celery_instance_if_connection_valid(self, flask_app):
        celery_instance = celery.init_celery(flask_app)
        assert isinstance(celery_instance, Celery)

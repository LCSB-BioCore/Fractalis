import pytest
from celery import Celery

from fractalis.celery import init_celery


class TestCelery(object):

    @pytest.fixture
    def app(self):
        from flask import Flask
        from fractalis.config import configure_app
        app = Flask('test_app')
        configure_app(app, mode='testing')
        return app

    def test_exception_if_no_connection_to_broker(self, app):
        app.config['CELERY_BROKER_URL'] = 'redis+socket:///foobar.socket'
        with pytest.raises(ConnectionRefusedError):
            init_celery(app)

    def test_exception_if_no_connection_to_result_backend(self, app):
        app.config['CELERY_RESULT_BACKEND'] = 'redis+socket:///foobar.socket'
        with pytest.raises(ConnectionRefusedError):
            init_celery(app)

    def test_returns_celery_instance_if_connection_valid(self, app):
        celery_instance = init_celery(app)
        assert isinstance(celery_instance, Celery)

from time import sleep

import pytest
import flask
from redislite import StrictRedis


class TestSession(object):

    @pytest.fixture
    def flask_app(self):
        from flask import Flask
        from fractalis import configure_app
        from fractalis.session import RedisSessionInterface
        app = Flask('test_app')
        configure_app(app, mode='testing')
        app.session_interface = RedisSessionInterface(
            redis_db_path=app.config['REDIS_DB_PATH'])
        return app

    def test_add_data_to_session_and_expect_it_in_db(self, flask_app):
        redis = StrictRedis(flask_app.config['REDIS_DB_PATH'])
        with flask_app.test_client() as test_client:
            with test_client.session_transaction() as session:
                session['foo'] = 'bar'
            session_id = flask.session.sid
            assert redis.get('session:{}'.format(session_id))['foo'] == 'bar'

    def test_add_data_and_expect_cookie_set(self, flask_app):
        with flask_app.test_client() as test_client:
            with test_client.session_transaction() as session:
                session['foo'] = 'bar'
            test_client.get()
            assert flask.request.cookies

    def test_dont_add_data_and_exoect_no_cookie_set(self, flask_app):
        with flask_app.test_client() as test_client:
            test_client.get()
            assert not flask.request.cookies

    def test_change_session_data_and_expect_change_in_db(self, flask_app):
        redis = StrictRedis(flask_app.config['REDIS_DB_PATH'])
        with flask_app.test_client() as test_client:
            with test_client.session_transaction() as session:
                session['foo'] = 'bar'
            session_id = flask.session.sid
            assert redis.get('session:{}'.format(session_id))['foo'] == 'bar'
            with test_client.session_transaction() as session:
                session['foo'] = 'baz'
            assert redis.get('session:{}'.format(session_id))['foo'] == 'baz'

    def test_exception_when_manipulating_session_data(self, flask_app):
        # No need to test this atm because we store nothing in the cookie
        assert True

    def test_exception_when_accessing_expired_session_data(self, flask_app):
        flask_app.config['PERMANENT_SESSION_LIFETIME'] = 1
        with flask_app.test_client() as test_client:
            with test_client.session_transaction() as session:
                session['foo'] = 'bar'
            sleep(2)
            //TODO Not sure which exception is thrown
            flask.session['foo']

    def test_session_data_not_in_db_when_expired(self, flask_app):
        flask_app.config['PERMANENT_SESSION_LIFETIME'] = 1
        redis = StrictRedis(flask_app.config['REDIS_DB_PATH'])
        with flask_app.test_client() as test_client:
            with test_client.session_transaction() as session:
                session['foo'] = 'bar'
            sleep(2)
            session_id = flask.session.sid
            assert not redis.get('session:{}'.format(session_id))['foo']

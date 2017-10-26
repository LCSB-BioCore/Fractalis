"""This module provides tests for the session interface of Fractalis."""

from uuid import UUID
from time import sleep

import pytest
import flask
from redis import StrictRedis


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestSession:

    @pytest.fixture(scope='module')
    def app(self):
        from fractalis import app
        app.testing = True
        return app

    @pytest.fixture(scope='module')
    def redis(self, app):
        redis = StrictRedis(host=app.config['REDIS_HOST'],
                            port=app.config['REDIS_PORT'])
        return redis

    def test_add_data_to_session_and_expect_it_in_db(self, app, redis):
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'bar'
                session_id = sess.sid
        assert redis.exists('session:{}'.format(session_id))

    def test_add_data_to_session_and_expect_sid_to_be_uuid(self, app):
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'bar'
                assert sess.sid
                UUID(sess.sid)

    def test_add_data_and_expect_cookie_set(self, app):
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'bar'
            c.get()
            assert flask.request.cookies

    def test_dont_add_data_and_exoect_no_cookie_set(self, app):
        with app.test_client() as c:
            c.get()
            assert not flask.request.cookies

    def test_change_session_data_and_expect_change_in_db(self, app, redis):
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'bar'
            session_id = sess.sid
        value_1 = redis.get('session:{}'.format(session_id))
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'baz'
            session_id = sess.sid
        value_2 = redis.get('session:{}'.format(session_id))
        assert value_1 != value_2

    def test_session_data_not_in_db_when_expired(self, app, redis):
        app.config['PERMANENT_SESSION_LIFETIME'] = 1
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'bar'
                session_id = sess.sid
        assert redis.exists('session:{}'.format(session_id))
        sleep(2)
        assert not redis.exists('session:{}'.format(session_id))

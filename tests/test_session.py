from uuid import UUID
from time import sleep

import pytest
import flask
from redis import StrictRedis


class TestSession(object):

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
        value = redis.get('session:{}'.format(session_id))
        assert flask.json.loads(value)['foo'] == 'bar'

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
        value = redis.get('session:{}'.format(session_id))
        assert flask.json.loads(value)['foo'] == 'bar'
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'baz'
            session_id = sess.sid
        value = redis.get('session:{}'.format(session_id))
        assert flask.json.loads(value)['foo'] == 'baz'

    def test_session_data_not_in_db_when_expired(self, app, redis):
        app.config['PERMANENT_SESSION_LIFETIME'] = 1
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess.permanent = True
                sess['foo'] = 'bar'
                session_id = sess.sid
        assert redis.get('session:{}'.format(session_id))
        sleep(2)
        assert not redis.get('session:{}'.format(session_id))

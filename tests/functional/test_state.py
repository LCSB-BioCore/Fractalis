"""This module tests the state controller module."""

import json

from uuid import UUID, uuid4
import flask
import pytest

from fractalis import redis, sync


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestState:

    @pytest.fixture(scope='function')
    def test_client(self):
        sync.cleanup_all()
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            sync.cleanup_all()

    def test_400_if_no_task_id_in_payload(self, test_client):
        rv = test_client.post('/state', data=flask.json.dumps('$...foo'))
        body = flask.json.loads(rv.get_data())
        assert 400 == rv.status_code, body
        assert 'error' in body
        assert 'contains no data task ids' in body['error']

    def test_400_if_task_id_not_in_redis(self, test_client):
        rv = test_client.post('/state', data=flask.json.dumps('$123$'))
        body = flask.json.loads(rv.get_data())
        assert 400 == rv.status_code, body
        assert 'error' in body
        assert 'could not be found in redis' in body['error']

    def test_400_if_task_id_in_redis_but_no_data_state(self, test_client):
        redis.set('data:123', '')
        rv = test_client.post('/state', data=flask.json.dumps('$123$'))
        body = flask.json.loads(rv.get_data())
        assert 400 == rv.status_code, body
        assert 'error' in body
        assert 'no valid data state' in body['error']

    def test_save_state_saves_and_returns(self, test_client):
        redis.set('data:123', json.dumps({'meta': {'descriptor': 'foo'}}))
        rv = test_client.post('/state',
                              data=flask.json.dumps({'test': ['$123$']}))
        body = flask.json.loads(rv.get_data())
        assert 201 == rv.status_code, body
        assert UUID(body['state_id'])
        state = json.loads(redis.get('state:{}'.format(body['state_id'])))
        assert 'test' in state
        assert ['$123$'] == state['test']

    def test_404_if_request_invalid_state_id(self, test_client):
        rv = test_client.post(
            '/state/{}'.format(str(uuid4())), data=flask.json.dumps(
                {'handler': '', 'server': '', 'auth': {'token': ''}}))
        assert 404 == rv.status_code
        body = flask.json.loads(rv.get_data())
        assert 'error' in body
        assert 'not find state associated with id' in body['error']

    def test_404_if_state_id_is_no_uuid(self, test_client):
        rv = test_client.post('/state/123')
        assert 404 == rv.status_code

    def test_400_if_payload_schema_incorrect(self, test_client):
        rv = test_client.post('/state/{}'.format(str(uuid4())),
                              data=flask.json.dumps({'foo': 123}))
        assert 400 == rv.status_code

    def test_error_if_task_id_is_no_etl_id(self, test_client):
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps({'foo': '$123$'}))
        rv = test_client.post('/state/{}'.format(uuid), data=flask.json.dumps(
            {'handler': '', 'server': '', 'auth': {'token': ''}}))
        body = flask.json.loads(rv.get_data())
        assert 403 == rv.status_code, body
        assert 'error' in body
        assert 'data task ids are missing' in body['error']
        assert not redis.exists('state:{}'.format(uuid))

    def test_202_create_valid_state_if_valid_conditions(self, test_client):
        uuid = str(uuid4())
        redis.set(name='data:123',
                  value=json.dumps(
                      {'meta': {'descriptor': {'data_type': 'default'}}}))
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps({'foo': '$123$'}))
        rv = test_client.post(
            '/state/{}'.format(uuid), data=flask.json.dumps(
                {'handler': 'test', 'server': 'foo', 'auth': {'token': ''}}))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        assert not body
        with test_client.session_transaction() as sess:
            assert sess['data_tasks']
            assert sess['state_access']
            assert sess['data_tasks'] == sess['state_access'][uuid]
            assert [UUID(uuid) for uuid in sess['data_tasks']]

    def test_404_if_get_non_existing_state(self, test_client):
        uuid = str(uuid4())
        rv = test_client.get('/state/{}'.format(uuid))
        body = flask.json.loads(rv.get_data())
        assert 404 == rv.status_code
        assert 'error' in body
        assert 'Cannot get state.' in body['error']

    def test_404_if_get_non_uuid_state(self, test_client):
        rv = test_client.get('/state/123')
        assert 404 == rv.status_code

    def test_404_if_get_not_previously_self_requested_state(self, test_client):
        uuid = str(uuid4())
        redis.set(name='data:{}'.format(uuid),
                  value=json.dumps(
                      {'meta': {'descriptor': {'data_type': 'default'}}}))
        rv = test_client.post('/state',
                              data=flask.json.dumps('${}$'.format(uuid)))
        body = flask.json.loads(rv.get_data())
        assert 201 == rv.status_code, body
        state_id = body['state_id']
        rv = test_client.post(
            '/state/{}'.format(state_id), data=flask.json.dumps(
                {'handler': 'test', 'server': 'foo', 'auth': {'token': ''}}))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        with test_client.session_transaction() as sess:
            del sess['state_access'][state_id]
        rv = test_client.get('/state/{}'.format(state_id))
        body = flask.json.loads(rv.get_data())
        assert 404 == rv.status_code, body
        assert 'error' in body
        assert 'Cannot get state.' in body['error']

    def test_403_if_etl_fails(self, test_client):
        redis.set(name='data:123',
                  value=json.dumps(
                      {'meta': {'descriptor': {'data_type': 'default'}}}))
        rv = test_client.post('/state', data=flask.json.dumps('$123$'))
        body = flask.json.loads(rv.get_data())
        state_id = body['state_id']
        test_client.post(
            '/state/{}'.format(state_id),
            data=flask.json.dumps(
                {'handler': 'test',
                 'server': 'foo',
                 'auth': {'token': 'fail'}}))
        rv = test_client.get('/state/{}?wait=1'.format(state_id))
        body = flask.json.loads(rv.get_data())
        assert 403 == rv.status_code, body
        assert 'error' in body
        assert 'ETLs failed' in body['error']

    def test_202_then_200_for_running_etl(self, test_client):
        redis.set(name='data:123',
                  value=json.dumps(
                      {'meta': {'descriptor': {'data_type': 'default'}}}))
        rv = test_client.post('/state', data=flask.json.dumps('$123$'))
        body = flask.json.loads(rv.get_data())
        state_id = body['state_id']
        test_client.post(
            '/state/{}'.format(state_id),
            data=flask.json.dumps(
                {'handler': 'test',
                 'server': 'foo',
                 'auth': {'token': ''}}))
        rv = test_client.get('/state/{}'.format(state_id))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        assert 'message' in body
        assert 'ETLs are still running.' in body['message']
        rv = test_client.get('/state/{}?wait=1'.format(state_id))
        body = flask.json.loads(rv.get_data())
        assert 200 == rv.status_code, body
        assert 'state' in body
        assert '$123$' in body['state']

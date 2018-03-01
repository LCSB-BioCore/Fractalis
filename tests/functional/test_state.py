"""This module tests the state controller module."""

import json

from uuid import UUID, uuid4
import flask
import pytest

from fractalis import redis, sync, celery
from fractalis.data.etlhandler import ETLHandler


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
        payload = {
            'state': {'abc': '$...foo'},
            'handler': 'test',
            'server': 'localfoo'
        }
        rv = test_client.post('/state', data=flask.json.dumps(payload))
        body = flask.json.loads(rv.get_data())
        assert 400 == rv.status_code, body
        assert 'error' in body
        assert 'contains no data task ids' in body['error']

    def test_400_if_task_id_not_in_redis(self, test_client):
        payload = {
            'state': {'abc': '${}$'.format(uuid4())},
            'handler': 'test',
            'server': 'localfoo'
        }
        rv = test_client.post('/state', data=flask.json.dumps(payload))
        body = flask.json.loads(rv.get_data())
        assert 400 == rv.status_code, body
        assert 'error' in body
        assert 'could not be found in redis' in body['error']

    def test_save_state_saves_and_returns(self, test_client):
        uuid = str(uuid4())
        payload = {
            'state': {'test': ['${}$'.format(uuid), '${${}']},
            'handler': 'test',
            'server': 'localfoo'
        }
        redis.set(name='data:{}'.format(uuid),
                  value=json.dumps({'meta': {'descriptor': 'foo'}}))
        rv = test_client.post('/state', data=flask.json.dumps(payload))
        body = flask.json.loads(rv.get_data())
        assert 201 == rv.status_code, body
        assert UUID(body['state_id'])
        meta_state = json.loads(redis.get('state:{}'.format(body['state_id'])))
        assert meta_state['task_ids'] == [uuid]
        assert meta_state['state']['test'][0] == '${}$'.format(uuid)

    def test_save_state_discards_duplicates(self, test_client):
        uuid1 = str(uuid4())
        uuid2 = str(uuid4())
        payload = {
            'state': {'test': ['${}$'.format(uuid1),
                               '${}$'.format(uuid1),
                               '${}$'.format(uuid2)]},
            'handler': 'test',
            'server': 'localfoo'
        }
        redis.set(name='data:{}'.format(uuid1),
                  value=json.dumps({'meta': {'descriptor': 'foo'}}))
        redis.set(name='data:{}'.format(uuid2),
                  value=json.dumps({'meta': {'descriptor': 'bar'}}))
        rv = test_client.post('/state', data=flask.json.dumps(payload))
        body = flask.json.loads(rv.get_data())
        assert 201 == rv.status_code, body
        assert UUID(body['state_id'])
        meta_state = json.loads(redis.get('state:{}'.format(body['state_id'])))
        assert len(meta_state['task_ids']) == 2
        assert len(meta_state['descriptors']) == 2
        assert uuid1 in meta_state['task_ids']
        assert uuid2 in meta_state['task_ids']
        assert 'foo' in meta_state['descriptors']
        assert 'bar' in meta_state['descriptors']

    def test_400_if_payload_schema_incorrect_1(self, test_client):
        payload = {
            'state': {'test': ['${}$'.format(uuid4())]},
            'server': 'localfoo'
        }
        rv = test_client.post('/state', data=flask.json.dumps(payload))
        assert 400 == rv.status_code

    def test_404_if_request_invalid_state_id(self, test_client):
        rv = test_client.post('/state/{}'.format(str(uuid4())),
                              data=flask.json.dumps({'auth': {'token': ''}}))
        assert 404 == rv.status_code
        body = flask.json.loads(rv.get_data())
        assert 'error' in body
        assert 'not find state associated with id' in body['error']

    def test_400_if_payload_schema_incorrect_2(self, test_client):
        payload = {
            'auth': {}
        }
        rv = test_client.post('/state/{}'.format(str(uuid4())),
                              data=flask.json.dumps(payload))
        assert 400 == rv.status_code

    def test_request_state_acces_works(self, test_client):
        meta_state = {
            'state': {'foo': ['$123$', '$456$']},
            'handler': 'test',
            'server': 'localfoo',
            'task_ids': ['123', '456'],
            'descriptors': [
                {'data_type': 'default'},
                {'data_type': 'default', 'foo': 'bar'}
            ],
        }
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps(meta_state))
        with test_client.session_transaction() as sess:
            assert not sess['data_tasks']
            assert not sess['state_access']
        rv = test_client.post('/state/{}'.format(uuid),
                              data=flask.json.dumps({'auth': {'token': ''}}))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        assert not body
        with test_client.session_transaction() as sess:
            assert len(sess['data_tasks']) == 2
            assert len(sess['state_access']) == 1
            key = list(sess['state_access'].keys())[0]
            assert len(sess['state_access'][key]) == 2
            assert sess['data_tasks'][0] in sess['state_access'][key]
            assert sess['data_tasks'][1] in sess['state_access'][key]
            assert meta_state['task_ids'][0] not in sess['state_access'][key]
            assert meta_state['task_ids'][1] not in sess['state_access'][key]

    def test_request_state_access_reuses_duplicate(
            self, test_client):
        uuid1 = str(uuid4())
        uuid2 = str(uuid4())
        meta_state = {
            'state': {'foo': ['${}$'.format(uuid1), '${}$'.format(uuid2)]},
            'handler': 'test',
            'server': 'localfoo',
            'task_ids': [uuid1, uuid2],
            'descriptors': [
                {'data_type': 'default'},
                {'data_type': 'default'}
            ],
        }
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps(meta_state))
        with test_client.session_transaction() as sess:
            assert not sess['data_tasks']
            assert not sess['state_access']
        rv = test_client.post('/state/{}'.format(uuid),
                              data=flask.json.dumps({'auth': {'token': ''}}))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        assert not body
        with test_client.session_transaction() as sess:
            assert len(sess['data_tasks']) == 1
            assert len(sess['state_access']) == 1
            key = list(sess['state_access'].keys())[0]
            assert len(sess['state_access'][key]) == 1
            assert sess['data_tasks'] == sess['state_access'][key]
            assert meta_state['task_ids'][0] != sess['state_access'][key][0]

    def test_request_state_reuses_previous_etls_but_only_in_own_scope(
            self, test_client, monkeypatch):
        uuid1 = str(uuid4())
        uuid2 = str(uuid4())
        descriptor_1 = {'data_type': 'default', 'id': 1}
        descriptor_2 = {'data_type': 'default', 'id': 2}
        handler = 'test'
        server = 'localfoo'
        meta_state = {
            'state': {'foo': ['${}$'.format(uuid1), '${}$'.format(uuid2)]},
            'handler': handler,
            'server': server,
            'task_ids': [uuid1, uuid2],
            'descriptors': [
                descriptor_1,
                descriptor_2
            ],
        }
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps(meta_state))
        etlhandler = ETLHandler.factory(handler=handler,
                                        server=server,
                                        auth={})
        etlhandler.create_redis_entry(task_id=uuid1,
                                      file_path='',
                                      descriptor=descriptor_1,
                                      data_type='')
        etlhandler.create_redis_entry(task_id=uuid2,
                                      file_path='',
                                      descriptor=descriptor_2,
                                      data_type='')
        with test_client.session_transaction() as sess:
            sess['data_tasks'] = [uuid2]

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'SUCCESS'
                self.id = args[0]

            def get(self, *args, **kwargs):
                pass

        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        rv = test_client.post('/state/{}'.format(uuid),
                              data=flask.json.dumps({'auth': {'token': ''}}))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        assert not body
        with test_client.session_transaction() as sess:
            assert len(sess['data_tasks']) == 2
            key = list(sess['state_access'].keys())[0]
            assert len(sess['state_access'][key]) == 2
            assert meta_state['task_ids'][0] not in sess['state_access'][key]
            assert meta_state['task_ids'][1] in sess['state_access'][key]

    def test_get_state_data_404_if_not_requested_before(self, test_client):
        rv = test_client.get('/state/{}'.format(str(uuid4())))
        assert 404 == rv.status_code
        body = flask.json.loads(rv.get_data())
        assert 'Cannot get state' in body.get('error')

    def test_get_state_with_replaced_ids_if_all_tasks_succeed(
            self, test_client, monkeypatch):
        meta_state = {
            'state': {'foo': ['$123$', '$456$']},
            'task_ids': ['123', '456'],
        }
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps(meta_state))
        with test_client.session_transaction() as sess:
            sess['state_access'][uuid] = ['abc', 'efg']

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'SUCCESS'
                self.id = args[0]

            def get(self, *args, **kwargs):
                pass
        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        rv = test_client.get('/state/{}'.format(uuid))
        body = flask.json.loads(rv.get_data())
        assert 200 == rv.status_code, body
        assert body['state']['foo'][0] == '$abc$'
        assert body['state']['foo'][1] == '$efg$'

    def test_get_state_get_message_if_not_all_tasks_finished(
            self, test_client, monkeypatch):
        meta_state = {
            'state': {'foo': ['$123$', '$456$']},
            'task_ids': ['123', '456'],
        }
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps(meta_state))
        with test_client.session_transaction() as sess:
            sess['state_access'][uuid] = ['abc', 'efg']

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'SUBMITTED'
                self.id = args[0]

            def get(self, *args, **kwargs):
                pass
        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        rv = test_client.get('/state/{}'.format(uuid))
        body = flask.json.loads(rv.get_data())
        assert 202 == rv.status_code, body
        assert 'still running' in body.get('message')

    def test_get_state_refuse_if_one_task_fails(
            self, test_client, monkeypatch):
        meta_state = {
            'state': {'foo': ['$123$', '$456$']},
            'task_ids': ['123', '456'],
        }
        uuid = str(uuid4())
        redis.set(name='state:{}'.format(uuid),
                  value=json.dumps(meta_state))
        with test_client.session_transaction() as sess:
            sess['state_access'][uuid] = ['abc', 'efg']

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'FAILURE'
                self.id = args[0]

            def get(self, *args, **kwargs):
                pass
        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        rv = test_client.get('/state/{}'.format(uuid))
        body = flask.json.loads(rv.get_data())
        assert 403 == rv.status_code, body
        assert 'no access' in body.get('error')

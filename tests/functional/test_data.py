"""This module tests the data controller module."""

import json
import os
from uuid import UUID, uuid4

import flask
import pytest

from fractalis import app, redis, sync


# noinspection PyMissingOrEmptyDocstring, PyMissingTypeHints
class TestData:

    @pytest.fixture(scope='function')
    def test_client(self):
        sync.cleanup_all()
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            sync.cleanup_all()

    @staticmethod
    def small_load(fail=False):
        return {
            'handler': 'test',
            'server': 'localhost:1234',
            'auth': {'token': 'fail' if fail else '7746391376142672192764'},
            'descriptors': [{
                'data_type': 'default',
                'concept': str(uuid4())
            }]
        }

    @staticmethod
    def big_load(fail=False):
        return {
            'handler': 'test',
            'server': 'localhost:1234',
            'auth': {'token': 'fail' if fail else '7746391376142672192764'},
            'descriptors': [
                {
                    'data_type': 'default',
                    'concept': str(uuid4())
                },
                {
                    'data_type': 'default',
                    'concept': str(uuid4())
                },
                {
                    'data_type': 'default',
                    'concept': str(uuid4())
                }
            ]
        }

    @pytest.fixture(scope='function', params=['small', 'big'])
    def payload(self, request):
        def _payload():
            load = self.small_load() if request.param == 'small' \
                else self.big_load()
            return {'size': len(load['descriptors']),
                    'serialized': flask.json.dumps(load)}
        return _payload

    @pytest.fixture(scope='function', params=['small', 'big'])
    def faiload(self, request):
        load = self.small_load(True) if request.param == 'small' \
            else self.big_load(True)
        return {'size': len(load['descriptors']),
                'serialized': flask.json.dumps(load)}

    @pytest.fixture(scope='function', params=[
        {
            'handler': '',
            'server': 'localhost',
            'auth': '{"''tok"n'"" '12345678"90',
            'descriptors': '[{"data_type": "default", "concept": "GSE1234"}]'
        },
        {
            'handler': 'test',
            'server': '',
            'auth': '{"token": "1234567890"}',
            'descriptors': '[{"data_type": "default", "concept": "GSE1234"}]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '',
            'descriptors': '[{"data_type": "default", "concept": "GSE1234"}]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{"token": "1234567890"}',
            'descriptors': ''
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{"token": "1234567890"}',
            'descriptors': '[{"data_type": "default", "concept": "GSE1234"}]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{"token": "1234567890"}',
            'descriptors': '[{"concept": "GSE1234"}]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{"token": "1234567890"}',
            'descriptors': '[{"data_type": "default"}]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{"token": "1234567890"}',
            'descriptors': '[{"data_type": "", "concept": "GSE1234"}]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{"token": "234567890"}',
            'descriptors': '[]'
        },
        {
            'handler': 'test',
            'server': 'localhost',
            'auth': '{}',
            'descriptors': '[{"data_type": "default", "concept": "GSE1234"}]'
        }
    ])
    def bad_post(self, test_client, request):
        return lambda: test_client.post('/data', data=flask.json.dumps({
            'handler': request.param['handler'],
            'server': request.param['server'],
            'auth': request.param['auth'],
            'descriptors': request.param['descriptors']
        }))

    def test_400_on_invalid_payload(self, bad_post):
        assert bad_post().status_code == 400

    def test_valid_response_on_post(self, test_client, payload):
        data = payload()
        rv = test_client.post('/data', data=data['serialized'])
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert not body

    def test_valid_redis_before_loaded_on_post(self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        keys = redis.keys('data:*')
        assert len(keys) == data['size']
        for key in keys:
            value = redis.get(key)
            data_state = json.loads(value)
            assert 'file_path' in data_state
            assert 'label' in data_state
            assert 'data_type' in data_state
            assert 'meta' in data_state

    def test_valid_redis_after_loaded_on_post(self, test_client, payload):
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        keys = redis.keys('data:*')
        assert len(keys) == data['size']
        for key in keys:
            value = redis.get(key)
            data_state = json.loads(value)
            assert 'file_path' in data_state
            assert 'label' in data_state
            assert 'data_type' in data_state
            assert 'meta' in data_state

    def test_valid_filesystem_before_loaded_on_post(
            self, test_client, payload):
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        data = payload()
        test_client.post('/data', data=data['serialized'])
        if os.path.exists(data_dir):
            assert len(os.listdir(data_dir)) == 0
        keys = redis.keys('data:*')
        for key in keys:
            value = redis.get(key)
            data_state = json.loads(value)
            assert not os.path.exists(data_state['file_path'])

    def test_valid_filesystem_after_loaded_on_post(
            self, test_client, payload):
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        assert len(os.listdir(data_dir)) == data['size']
        for f in os.listdir(data_dir):
            assert UUID(f)
        keys = redis.keys('data:*')
        for key in keys:
            value = redis.get(key)
            data_state = json.loads(value)
            assert os.path.exists(data_state['file_path'])

    def test_valid_session_on_post(self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        with test_client.session_transaction() as sess:
            assert len(sess['data_tasks']) == data['size']

    def test_session_matched_redis_in_post_big_payload(
            self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        with test_client.session_transaction() as sess:
            for task_id in sess['data_tasks']:
                assert redis.exists('data:{}'.format(task_id))

    def test_many_post_and_valid_state(self, test_client, payload):
        requests = 5
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        size = 0
        for i in range(requests):
            data = payload()
            size += data['size']
            rv = test_client.post('/data?wait=1', data=data['serialized'])
            assert rv.status_code == 201
        assert len(os.listdir(data_dir)) == size
        assert len(redis.keys('data:*')) == size

    def test_valid_response_before_loaded_on_get(self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        rv = test_client.get('/data')
        assert rv.status_code == 200
        body = flask.json.loads(rv.get_data())
        for data_state in body['data_states']:
            assert 'file_path' not in data_state
            assert 'meta' not in data_state
            assert data_state['etl_state'] == 'SUBMITTED'
            assert not data_state['etl_message']
            assert data_state['data_type'] == 'mock'
            assert 'task_id' in data_state

    def test_valid_response_after_loaded_on_get(self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        rv = test_client.get('/data?wait=1')
        assert rv.status_code == 200
        body = flask.json.loads(rv.get_data())
        for data_state in body['data_states']:
            assert 'file_path' not in data_state
            assert data_state['etl_state'] == 'SUCCESS'
            assert not data_state['etl_message']
            assert data_state['data_type'] == 'mock'
            assert 'task_id' in data_state

    def test_discard_expired_states(self, test_client):
        data_state = {
            'a': 'b',
            'file_path': '',
            'meta': ''
        }
        redis.set(name='data:456', value=json.dumps(data_state))
        with test_client.session_transaction() as sess:
            sess['data_tasks'] = ['123', '456']
        rv = test_client.get('/data?wait=1')
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 200, body
        assert len(body['data_states']) == 1
        assert body['data_states'][0]['a'] == 'b'
        with test_client.session_transaction() as sess:
            sess['data_tasks'] = ['456']

    def test_valid_response_if_failing_on_get(self, test_client, faiload):
        test_client.post('/data', data=faiload['serialized'])
        rv = test_client.get('/data?wait=1')
        assert rv.status_code == 200
        body = flask.json.loads(rv.get_data())
        for data_state in body['data_states']:
            assert 'file_path' not in data_state
            assert data_state['etl_state'] == 'FAILURE'
            assert data_state['etl_message']
            assert data_state['data_type'] == 'mock'
            assert 'task_id' in data_state

    def test_valid_state_for_finished_etl_on_delete(
            self, test_client, payload):
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            os.path.exists(data_state['file_path'])
            test_client.delete('/data/{}?wait=1'.format(data_state['task_id']))
            assert not redis.exists(key)
            assert not os.path.exists(data_state['file_path'])
            with test_client.session_transaction() as sess:
                assert data_state['task_id'] not in sess['data_tasks']

    def test_valid_state_for_running_etl_on_delete(self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            assert not os.path.exists(data_state['file_path'])
            test_client.delete('/data/{}?wait=1'.format(data_state['task_id']))
            assert not redis.exists(key)
            assert not os.path.exists(data_state['file_path'])
            with test_client.session_transaction() as sess:
                assert data_state['task_id'] not in sess['data_tasks']

    def test_valid_state_for_failed_etl_on_delete(self, test_client, faiload):
        test_client.post('/data?wait=1', data=faiload['serialized'])
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            assert not os.path.exists(data_state['file_path'])
            test_client.delete('/data/{}?wait=1'.format(data_state['task_id']))
            assert not redis.exists(key)
            assert not os.path.exists(data_state['file_path'])
            with test_client.session_transaction() as sess:
                assert data_state['task_id'] not in sess['data_tasks']

    def test_403_if_no_auth_on_delete(self, test_client, payload):
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        with test_client.session_transaction() as sess:
            sess['data_tasks'] = []
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            assert os.path.exists(data_state['file_path'])
            rv = test_client.delete('/data/{}?wait=1'
                                    .format(data_state['task_id']))
            body = flask.json.loads(rv.get_data())
            assert rv.status_code == 403
            assert 'Refusing access.' in body['error']
            assert redis.exists(key)
            assert os.path.exists(data_state['file_path'])

    def test_valid_state_for_finished_etl_on_delete_all(
            self, test_client, payload):
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        test_client.delete('/data?wait=1')
        assert not redis.keys('data:*')
        assert len(os.listdir(data_dir)) == 0
        with test_client.session_transaction() as sess:
            assert not sess['data_tasks']

    def test_encryption_works(self, test_client, payload):
        app.config['FRACTALIS_ENCRYPT_CACHE'] = True
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        keys = redis.keys('data:*')
        for key in keys:
            value = redis.get(key)
            data_state = json.loads(value)
            file_path = data_state['file_path']
            with pytest.raises(UnicodeDecodeError):
                open(file_path, 'r').readlines()
        app.config['FRACTALIS_ENCRYPT_CACHE'] = False

    def test_valid_response_before_loaded_on_meta(self, test_client, payload):
        data = payload()
        test_client.post('/data', data=data['serialized'])
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            rv = test_client.get('/data/meta/{}'.format(data_state['task_id']))
            body = flask.json.loads(rv.get_data())
            assert rv.status_code == 200
            assert 'features' not in body['meta']

    def test_valid_response_after_loaded_on_meta(self, test_client, payload):
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            rv = test_client.get('/data/meta/{}?wait=1'
                                 .format(data_state['task_id']))
            body = flask.json.loads(rv.get_data())
            assert rv.status_code == 200
            assert 'features' in body['meta']

    def test_403_if_no_auth_on_get_meta(self, test_client, payload):
        data = payload()
        test_client.post('/data?wait=1', data=data['serialized'])
        with test_client.session_transaction() as sess:
            sess['data_tasks'] = []
        for key in redis.keys('data:*'):
            value = redis.get(key)
            data_state = json.loads(value)
            rv = test_client.get('/data/meta/{}?wait=1'
                                 .format(data_state['task_id']))
            body = flask.json.loads(rv.get_data())
            assert rv.status_code == 403
            assert 'Refusing access.' in body['error']
            assert redis.exists(key)
            assert os.path.exists(data_state['file_path'])

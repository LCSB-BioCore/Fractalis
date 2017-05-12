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
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            sync.cleanup_all()

    @staticmethod
    def small_payload(random=True):
        return flask.json.dumps({
            'handler': 'test',
            'server': 'localhost:1234',
            'auth': {'token': '7746391376142672192764'},
            'descriptors': [{
                'data_type': 'default',
                'concept': str(uuid4()) if random else 'concept'
            }]
        })

    @staticmethod
    def big_payload(random=True):
        return flask.json.dumps({
            'handler': 'test',
            'server': 'localhost:1234',
            'auth': {'token': '7746391376142672192764'},
            'descriptors': [
                {
                    'data_type': 'default',
                    'concept': str(uuid4()) if random else 'concept'
                },
                {
                    'data_type': 'default',
                    'concept': str(uuid4()) if random else 'concept'
                },
                {
                    'data_type': 'default',
                    'concept': str(uuid4()) if random else 'concept'
                }
            ]
        })

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

    def test_valid_response_on_post_small_payload(
            self, test_client, small_payload):
        rv = test_client.post('/data?wait=1', small_payload())
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert len(body['job_ids']) == 1
        assert UUID(body['job_ids'][0])

    def test_valid_redis_on_post_small_payload(
            self, test_client, small_payload):
        test_client.post('/data?wait=1', small_payload())
        keys = redis.keys('data:*')
        assert len(keys) == 1
        value = redis.get(keys[0])
        data_obj = json.loads(value.decode('utf-8'))
        assert 'file_path' in data_obj
        assert 'last_access' in data_obj
        assert 'label' in data_obj
        assert 'descriptor' in data_obj
        assert 'data_type' in data_obj

    def test_valid_filesystem_on_post_small_payload(
            self, test_client, small_payload):
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        test_client.post('/data?wait=1', small_payload())
        keys = redis.keys('data:*')
        value = redis.get(keys[0])
        data_obj = json.loads(value.decode('utf-8'))
        assert len(os.listdir(data_dir)) == 1
        assert os.path.exists(data_obj['file_path'])

    def test_valid_response_on_post_big_payload(
            self, test_client, big_payload):
        rv = test_client.post('/data?wait=1', big_payload())
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert len(body['job_ids']) == 3
        assert UUID(body['job_ids'][0])
        assert UUID(body['job_ids'][1])
        assert UUID(body['job_ids'][2])

    def test_valid_redis_on_post_big_payload(
            self, test_client, big_payload):
        test_client.post('/data?wait=1', big_payload())
        keys = redis.keys('data:*')
        assert len(keys) == 3
        for key in keys:
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert 'file_path' in data_obj
            assert 'last_access' in data_obj
            assert 'label' in data_obj
            assert 'descriptor' in data_obj
            assert 'data_type' in data_obj

    def test_valid_filesystem_on_post_big_payload(
            self, test_client, big_payload):
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        test_client.post('/data?wait=1', big_payload())
        assert len(os.listdir(data_dir)) == 3
        keys = redis.keys('data:*')
        for key in keys:
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert os.path.exists(data_obj['file_path'])









    def test_201_on_big_POST_and_valid_state(self, test_client, big_post):
        rv = big_post(random=False)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        assert len(body['data_ids']) == 3
        assert test_client.head('/data?wait=1').status_code == 200
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        assert len(os.listdir(data_dir)) == 3
        for f in os.listdir(data_dir):
            assert UUID(f)
        data = redis.keys('data:*')
        assert len(data) == 3
        for key in data:
            key = key.decode('utf-8')
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']
            assert redis.exists('shadow:{}'.format(key))

    def test_many_small_POST_and_valid_state(self, test_client, small_post):
        N = 10
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        for i in range(N):
            rv = small_post(random=False)
            assert rv.status_code == 201
            body = flask.json.loads(rv.get_data())
            assert len(body['data_ids']) == 1
        assert test_client.head('/data?wait=1').status_code == 200
        assert len(os.listdir(data_dir)) == 1
        for f in os.listdir(data_dir):
            assert UUID(f)
        data = redis.keys('data:*')
        assert len(data) == 1
        for key in data:
            key = key.decode('utf-8')
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']
            assert redis.exists('shadow:{}'.format(key))

    def test_many_small_random_POST_and_valid_state(
            self, test_client, small_post):
        N = 10
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        for i in range(N):
            rv = small_post(random=True)
            assert rv.status_code == 201
            body = flask.json.loads(rv.get_data())
            assert len(body['data_ids']) == 1
        assert test_client.head('/data?wait=1').status_code == 200
        assert len(os.listdir(data_dir)) == N
        for f in os.listdir(data_dir):
            assert UUID(f)
        data = redis.keys('data:*')
        assert len(data) == N
        for key in data:
            key = key.decode('utf-8')
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']
            assert redis.exists('shadow:{}'.format(key))

    def test_many_big_POST_and_valid_state(self, test_client, big_post):
        N = 10
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        for i in range(N):
            rv = big_post(random=False)
            assert rv.status_code == 201
            body = flask.json.loads(rv.get_data())
            assert len(body['data_ids']) == 3
        assert test_client.head('/data?wait=1').status_code == 200
        assert len(os.listdir(data_dir)) == 3
        for f in os.listdir(data_dir):
            assert UUID(f)
        data = redis.keys('data:*')
        assert len(data) == 3
        for key in data:
            key = key.decode('utf-8')
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']
            assert redis.exists('shadow:{}'.format(key))

    def test_many_big_random_POST_and_valid_state(self, test_client, big_post):
        N = 10
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        for i in range(N):
            rv = big_post(random=True)
            assert rv.status_code == 201
            body = flask.json.loads(rv.get_data())
            assert len(body['data_ids']) == 3
        assert test_client.head('/data?wait=1').status_code == 200
        assert len(os.listdir(data_dir)) == 3 * N
        for f in os.listdir(data_dir):
            assert UUID(f)
        data = redis.keys('data:*')
        assert len(data) == 3 * N
        for key in data:
            key = key.decode('utf-8')
            value = redis.get(key)
            data_obj = json.loads(value.decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']
            assert redis.exists('shadow:{}'.format(key))
            assert data_obj['data_type']

    def test_GET_by_id_and_valid_response(self, test_client, big_post):
        rv = big_post(random=False)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 3
        for data_id in data_ids:
            rv = test_client.get('/data/{}'.format(data_id))
            body = flask.json.loads(rv.get_data())
            data_state = body['data_state']
            assert rv.status_code == 200, data_state
            assert len(data_state) == 7  # include only minimal data in response
            assert not data_state['message']
            assert data_state['state']
            assert data_state['job_id']
            assert data_state['data_type']
            assert data_state['label']
            assert data_state['descriptor']
            assert data_state['data_id']

    def test_GET_by_all_and_valid_response(self, test_client, big_post):
        rv = big_post(random=False)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 3

        rv = test_client.get('/data')
        body = flask.json.loads(rv.get_data())

        for data_state in body['data_states']:
            assert len(data_state) == 7
            assert not data_state['message']
            assert data_state['state']
            assert data_state['job_id']
            assert data_state['data_type']
            assert data_state['label']
            assert data_state['descriptor']
            assert data_state['data_id']

    def test_GET_by_params_and_valid_response(self, test_client):
        data = dict(
            handler='test',
            server='localhost:1234',
            auth={'token': '7746391376142672192764'},
            descriptors=[
                {
                    'data_type': 'default',
                    'concept': str(uuid4())
                }
            ]
        )
        rv = test_client.post('/data', data=flask.json.dumps(data))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        payload = flask.json.dumps(dict(server=data['server'],
                                        descriptor=data['descriptors'][0]))
        rv = test_client.get('/data/{}'.format(payload))
        body = flask.json.loads(rv.get_data())
        data_state = body['data_state']
        assert rv.status_code == 200, data_state
        assert len(data_state) == 7  # include only minimal data in response
        assert not data_state['message']
        assert data_state['state']
        assert data_state['job_id']
        assert data_state['data_type']
        assert data_state['label']
        assert data_state['descriptor']
        assert data_state['data_id']

    def test_404_on_GET_by_id_if_no_auth(self, test_client, big_post):
        rv = big_post(random=False)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 3

        with test_client.session_transaction() as sess:
            sess['data_ids'].remove(data_ids[2])

        rv = test_client.get('/data/{}'.format(data_ids[0]))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 200, body

        rv = test_client.get('/data/{}'.format(data_ids[1]))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 200, body

        rv = test_client.get('/data/{}'.format(data_ids[2]))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 404, body

    def test_404_on_GET_by_params_if_no_auth(self, test_client):
        data = dict(
            handler='test',
            server='localhost:1234',
            auth={'token': '7746391376142672192764'},
            descriptors=[
                {
                    'data_type': 'default',
                    'concept': str(uuid4())
                }
            ]
        )
        rv = test_client.post('/data', data=flask.json.dumps(data))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body

        with test_client.session_transaction() as sess:
            sess['data_ids'] = []

        payload = flask.json.dumps(dict(server=data['server'],
                                        descriptor=data['descriptors'][0]))
        rv = test_client.get('/data/{}'.format(payload))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 404, body

    def test_empty_response_for_empty_session(self, test_client, small_post):
        rv = small_post(random=False)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body

        rv = test_client.get('/data')
        body = flask.json.loads(rv.get_data())
        assert body, body

        with test_client.session_transaction() as sess:
            sess['data_ids'] = []

        rv = test_client.get('/data')
        body = flask.json.loads(rv.get_data())
        assert not body['data_states'], body

    def test_exception_on_na_handler(self, test_client):
        data = dict(
            handler='abc',
            server='localhost:1234',
            auth={'token': '7746391376142672192764'},
            descriptors=[
                {
                    'data_type': 'default',
                    'concept': str(uuid4())
                }
            ]
        )
        with pytest.raises(NotImplementedError):
            test_client.post('/data', data=flask.json.dumps(data))

    def test_exception_on_na_etl(self, test_client):
        data = dict(
            handler='test',
            server='localhost:1234',
            auth={'token': '7746391376142672192764'},
            descriptors=[
                {
                    'data_type': 'abc',
                    'concept': str(uuid4())
                }
            ]
        )
        with pytest.raises(NotImplementedError):
            test_client.post('/data', data=flask.json.dumps(data))

    def test_session_data_ids_should_be_unique(self, test_client, small_post):
        small_post(random=False)
        small_post(random=False)
        with test_client.session_transaction() as sess:
            assert len(sess['data_ids']) == 1

    def test_no_access_on_failure(self, test_client):
        rv = test_client.post(
            '/data', data=flask.json.dumps(dict(
                handler='test',
                server='localhost:1234',
                auth={'token': '7746391376142672192764'},
                descriptors=[
                    {
                        'data_type': 'default',
                        'concept': 'test'
                    }
                ]
            )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        assert len(body['data_ids']) == 1
        data_id = body['data_ids'][0]

        rv = test_client.get('/data/{}'.format(data_id))
        assert rv.status_code == 200

        with test_client.session_transaction() as sess:
            sess['data_ids'] = []

        rv = test_client.get('/data/{}'.format(data_id))
        assert rv.status_code == 404

    def test_delete_and_no_db_entries(self, test_client, small_post):
        rv = small_post(random=True)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 1
        data_id = data_ids[0]
        data_obj = redis.get('data:{}'.format(data_id))
        assert data_obj
        assert redis.exists('shadow:data:{}'.format(data_id))
        test_client.delete('/data/{}?wait=1'.format(data_id))
        assert not redis.exists('data:{}'.format(data_id))
        assert not redis.exists('shadow:data:{}'.format(data_id))

    def test_delete_and_no_files(self, test_client, small_post):
        rv = small_post(random=True)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 1
        data_id = data_ids[0]
        test_client.get('/data/{}?wait=1'.format(data_id))
        value = redis.get('data:{}'.format(data_id))
        data_obj = json.loads(value.decode('utf-8'))
        assert os.path.exists(data_obj['file_path'])
        test_client.delete('/data/{}?wait=1'.format(data_id))
        assert not os.path.exists(data_obj['file_path'])

    def test_delete_and_no_id_in_session(self, test_client, small_post):
        rv = small_post(random=True)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 1
        data_id = data_ids[0]
        test_client.get('/data/{}?wait=1'.format(data_id))
        with test_client.session_transaction() as sess:
            assert data_id in sess['data_ids']
        test_client.delete('/data/{}?wait=1'.format(data_id))
        test_client.get('/data/{}?wait=1'.format(data_id))
        with test_client.session_transaction() as sess:
            assert data_id not in sess['data_ids']

    def test_cannot_delete_without_auth(self, test_client, small_post):
        rv = small_post(random=True)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 1
        data_id = data_ids[0]
        test_client.get('/data/{}?wait=1'.format(data_id))
        with test_client.session_transaction() as sess:
            sess['data_ids'] = []
        rv = test_client.delete('/data/{}?wait=1'.format(data_id))
        assert rv.status_code == 404

    def test_delete_all_and_no_id_in_session(self, test_client, small_post):
        rv = small_post(random=True)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        data_ids = body['data_ids']
        assert len(data_ids) == 1
        data_id = data_ids[0]
        test_client.get('/data/{}?wait=1'.format(data_id))
        with test_client.session_transaction() as sess:
            assert data_id in sess['data_ids']
        test_client.delete('/data/{}?wait=1'.format(data_id))
        test_client.get('/data/{}?wait=1'.format(data_id))
        with test_client.session_transaction() as sess:
            assert data_id not in sess['data_ids']
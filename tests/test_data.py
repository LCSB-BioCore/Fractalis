"""This module tests the data controller module."""

import os
import json
from glob import glob
from uuid import UUID, uuid4

import flask
import pytest

from fractalis import redis
from fractalis import app


class TestData:

    def cleanup(self):
        redis.flushall()
        data_dir = os.path.join(
            app.config['FRACTALIS_TMP_DIR'], 'data', '*')
        files = glob(data_dir)
        for f in files:
            os.remove(f)

    @pytest.fixture(scope='function')
    def test_client(self):
        self.cleanup()
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            self.cleanup()

    @pytest.fixture(scope='function')
    def small_post(self, test_client):
        return lambda random: test_client.post(
            '/data', data=flask.json.dumps(dict(
                handler='test',
                server='localhost:1234',
                token='7746391376142672192764',
                descriptors=[
                    {
                        'data_type': 'foo',
                        'concept': uuid4() if random else 'concept'
                    }
                ]
            )))

    @pytest.fixture(scope='function')
    def big_post(self, test_client):
        return lambda random: test_client.post(
            '/data', data=flask.json.dumps(dict(
                handler='test',
                server='localhost:1234',
                token='7746391376142672192764',
                descriptors=[
                    {
                        'data_type': 'foo',
                        'concept': uuid4() if random else 'concept1'
                    },
                    {
                        'data_type': 'bar',
                        'concept': uuid4() if random else 'concept2'
                    },
                    {
                        'data_type': 'foo',
                        'concept': uuid4() if random else 'concept3'
                    },
                ]
            )))

    # POST /

    def test_201_on_small_POST_and_file_exists(self, test_client, small_post):
        rv = small_post(random=False)
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert len(body['data_ids']) == 1
        test_client.head('/data?wait=1')
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        assert len(os.listdir(data_dir)) == 1
        assert UUID(os.listdir(data_dir)[0])

    def test_201_on_big_POST_and_files_exist(self, test_client, big_post):
        rv = big_post(random=False)
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert len(body['data_ids']) == 3
        test_client.head('/data?wait=1')
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        assert len(os.listdir(data_dir)) == 3
        for f in os.listdir(data_dir):
            assert UUID(f)

    def test_small_POST_and_valid_db_state(self, small_post):
        small_post(random=False)
        data = redis.hgetall(name='data')
        assert len(data) == 1
        for key in data:
            data_obj = json.loads(data[key].decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']

    def test_big_POST_and_valid_db_state(self, big_post):
        big_post(random=False)
        data = redis.hgetall(name='data')
        assert len(data) == 3
        for key in data:
            data_obj = json.loads(data[key].decode('utf-8'))
            assert data_obj['job_id']
            assert data_obj['file_path']

    def test_many_small_POST_and_files_exist(self, test_client, small_post):
        N = 10
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        for i in range(N):
            rv = small_post(random=False)
            assert rv.status_code == 201
            body = flask.json.loads(rv.get_data())
            assert len(body['data_ids']) == 1
        test_client.head('/data/?wait=1')
        assert len(os.listdir(data_dir)) == 1
        for f in os.listdir(data_dir):
            assert UUID(f)

    def test_many_small_random_POST_and_files_exist(
            self, test_client, small_post):
        N = 10
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        for i in range(N):
            rv = small_post(random=True)
            assert rv.status_code == 201
            body = flask.json.loads(rv.get_data())
            assert len(body['data_ids']) == 1
        test_client.head('/data/?wait=1')
        assert len(os.listdir(data_dir)) == N
        for f in os.listdir(data_dir):
            assert UUID(f)

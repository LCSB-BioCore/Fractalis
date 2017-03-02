"""This module tests the data controller module."""

import os
from glob import glob
from uuid import UUID

import flask
import pytest

from fractalis import redis
from fractalis import app


class TestData:

    @pytest.fixture(scope='function')
    def test_client(self):
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            redis.flushall()
            data_dir = os.path.join(
                app.config['FRACTALIS_TMP_DIR'], 'data', '*')
            files = glob(data_dir)
            for f in files:
                os.remove(f)

    @pytest.fixture(scope='function')
    def small_post(self, test_client):
        return test_client.post('/data', data=flask.json.dumps(dict(
            handler='test',
            server='localhost:1234',
            token='7746391376142672192764',
            descriptors=[
                {
                    'data_type': 'foo',
                    'concept': 'abc//efg/whatever'
                }
            ]
        )))

    @pytest.fixture(scope='function')
    def big_post(self, test_client):
        return test_client.post('/data', data=flask.json.dumps(dict(
            handler='test',
            server='localhost:1234',
            token='7746391376142672192764',
            descriptors=[
                {
                    'data_type': 'foo',
                    'concept': 'abc//efg/whatever'
                },
                {
                    'data_type': 'bar',
                    'concept': 'abc//efg/everwhat'
                },
                {
                    'data_type': 'foo',
                    'concept': 'abc//efg//xyz'
                },
            ]
        )))

    # POST /

    def test_201_on_small_POST_and_file_exists(self, test_client, small_post):
        rv = small_post
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert len(body['data_ids']) == 1
        data_id = body['data_ids'][0]
        test_client.head('/data/{}?wait=1'.format(data_id))
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        assert len(os.listdir(data_dir)) == 1
        assert UUID(os.listdir(data_dir)[0])

    def test_201_on_big_POST_and_files_exist(self, test_client, big_post):
        rv = big_post
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        assert len(body['data_ids']) == 3
        test_client.head('/data?wait=1')
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        assert len(os.listdir(data_dir)) == 3
        for f in os.listdir(data_dir):
            assert UUID(f)

"""This module tests the state controller module."""

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

    def test_save_state_saves_and_returns(self, test_client):
        rv = test_client.post('/state', data=flask.json.dumps('test'))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        assert UUID(body['state_id'])
        assert redis.get('state:{}'.format(body['state_id'])) == 'test'

    def test_404_if_request_invalid_state_id(self, test_client):
        rv = test_client.post('/state/{}'.format(str(uuid4())))
        assert rv.status_code == 404

    def test_error_if_state_id_is_no_uuid(self, test_client):
        rv = test_client.post('/state/123')
        assert rv.status_code == 400

    def test_error_if_task_id_is_no_etl_id(self, test_client):
        uuid = str(uuid4())
        redis.set('state:{}'.format(uuid), 'foo')
        rv = test_client.post('/state/{}'.format(uuid))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 400, body
        assert 'error' in body
        assert 'given payload cannot be saved' in body['error']

    def test_201_and_valid_state_if_valid_conditions(self, test_client):
        uuid = str(uuid4())
        redis.set('state:{}'.format(uuid), {'meta': {'descriptor': 'foo'}})
        rv = test_client.post('/state/{}'.format(uuid))
        assert rv.status_code == 201
        with test_client.session_transaction() as sess:
            assert sess['data_tasks']
            assert sess['state_access']
            assert sess['data_tasks'] == sess['state_access']
            assert [UUID(uuid) for id in sess['data_tasks']]
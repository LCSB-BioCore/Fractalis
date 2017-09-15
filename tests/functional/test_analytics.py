"""This module tests the analytics controller module."""

import json
import time
import uuid
from uuid import uuid4

import flask
import pytest

from fractalis import sync, app


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestAnalytics:

    @pytest.fixture(scope='function')
    def test_client(self):
        sync.cleanup_all()
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            sync.cleanup_all()

    @pytest.fixture(scope='function')
    def small_data_post(self, test_client):
        return lambda random, wait=0: test_client.post(
            '/data?wait={}'.format(wait), data=flask.json.dumps(dict(
                handler='test',
                server='localhost:1234',
                auth={'token': '7746391376142672192764'},
                descriptors=[
                    {
                        'data_type': 'default',
                        'concept': str(uuid4()) if random else 'concept'
                    }
                ]
            )))

    def test_new_resource_created(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='add_test_task',
            args={'a': 1, 'b': 2}
        )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        new_url = '/analytics/{}'.format(body['task_id'])
        assert uuid.UUID(body['task_id'])
        assert test_client.head(new_url).status_code == 200

    @pytest.fixture(scope='function',
                    params=[{'task_name': 'i_dont_exist_task',
                             'args': {'a': 1, 'b': 2}},
                            {'task_name': '',
                             'args': {'a': 1, 'b': 2}}])
    def bad_request(self, test_client, request):
        return test_client.post('/analytics',
                                data=flask.json.dumps(request.param))

    def test_400_if_post_body_invalid(self, bad_request):
        assert bad_request.status_code == 400

    def test_no_conflict_when_running_task_twice(self, test_client):
        rv1 = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='nothing_test_task',
            args={'seconds': 4}
        )))
        assert rv1.status_code == 201

        rv2 = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='nothing_test_task',
            args={'seconds': 4}
        )))
        assert rv2.status_code == 201

        body1 = flask.json.loads(rv1.get_data())
        new_url1 = '/analytics/{}?wait=0'.format(body1['task_id'])
        new_response1 = test_client.get(new_url1)
        assert new_response1.status_code == 200
        new_body1 = flask.json.loads(new_response1.get_data())
        assert new_body1['state'] != 'FAILURE'

        body2 = flask.json.loads(rv2.get_data())
        new_url2 = '/analytics/{}?wait=0'.format(body2['task_id'])
        new_response2 = test_client.get(new_url2)
        assert new_response2.status_code == 200
        new_body2 = flask.json.loads(new_response2.get_data())
        assert new_body2['state'] != 'FAILURE'

    def test_403_if_no_session_auth(self, test_client, small_data_post):
        small_data_post(random=False)
        with test_client.session_transaction() as sess:
            assert len(sess['data_tasks']) == 1
            task_id = sess['data_tasks'][0]
        with test_client.session_transaction() as sess:
            sess['data_tasks'] = []
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='sum_df_test_task',
            args={'a': '${}$'.format(task_id)}
        )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        url = '/analytics/{}?wait=1'.format(body['task_id'])
        rv = test_client.get(url)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 200, body
        assert body['state'] == 'FAILURE', body
        assert 'PermissionError' in body['result'], body

    def test_resource_deleted(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='add_test_task',
            args={'a': 1, 'b': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        assert test_client.head(new_url).status_code == 200
        assert test_client.delete(new_url).status_code == 200
        assert test_client.head(new_url).status_code == 403

    def test_403_if_deleting_non_existing_resource(self, test_client):
        rv = test_client.delete('/analytics/{}?wait=1'.format(str(uuid4())))
        assert rv.status_code == 403

    def test_running_resource_deleted(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='nothing_test_task',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        assert test_client.head(new_url).status_code == 200
        assert test_client.delete(new_url).status_code == 200
        assert test_client.head(new_url).status_code == 403

    def test_403_if_deleting_without_auth(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='nothing_test_task',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        time.sleep(1)
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        with test_client.session_transaction() as sess:
            sess['analytic_tasks'] = []
        assert test_client.delete(new_url).status_code == 403

    def test_status_contains_result_if_finished(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='add_test_task',
            args={'a': 1, 'b': 2}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'SUCCESS', new_body
        assert json.loads(new_body['result'])['sum'] == 3, new_body

    def test_status_result_empty_if_not_finished(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='nothing_test_task',
            args={'seconds': 4}
        )))
        time.sleep(1)
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=0'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert not new_body['result']
        assert new_body['state'] == 'SUBMITTED'

    def test_correct_response_if_task_fails(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='div_test_task',
            args={'a': 2, 'b': 0}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'FAILURE'
        assert 'ZeroDivisionError' in new_body['result']

    def test_403_if_status_non_existing_resource(self, test_client):
        assert test_client.get('/analytics/{}?wait=1'
                               .format(str(uuid4()))).status_code == 403

    def test_403_if_status_without_auth(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='nothing_test_task',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=0'.format(body['task_id'])
        with test_client.session_transaction() as sess:
            sess['analytic_tasks'] = []
        assert test_client.get(new_url).status_code == 403

    def test_float_when_summing_up_df(self, test_client, small_data_post):
        data_tasks = []

        small_data_post(random=True, wait=1)
        with test_client.session_transaction() as sess:
            data_tasks += sess['data_tasks']
            assert len(data_tasks) == 1

        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='sum_df_test_task',
            args={'a': '${}$'.format(data_tasks[0])}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_response.status_code == 200, new_body
        assert new_body['state'] == 'SUCCESS', new_body
        assert float(json.loads(new_body['result'])['sum'])

    def test_float_when_summing_up_encrypted_df(
            self, test_client, small_data_post):
        app.config['FRACTALIS_ENCRYPT_CACHE'] = True
        data_tasks = []
        small_data_post(random=True, wait=1)
        with test_client.session_transaction() as sess:
            data_tasks += sess['data_tasks']
            assert len(data_tasks) == 1

        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='sum_df_test_task',
            args={'a': '${}$'.format(data_tasks[0])}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_response.status_code == 200, new_body
        assert new_body['state'] == 'SUCCESS', new_body
        assert float(json.loads(new_body['result'])['sum'])
        app.config['FRACTALIS_ENCRYPT_CACHE'] = False

    def test_exception_if_result_not_json(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='invalid_json_task',
            args={'a': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'FAILURE'
        assert 'TypeError' in new_body['result']

    def test_exception_if_result_not_dict(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='no_dict_task',
            args={'a': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'FAILURE'
        assert 'TypeError' in new_body['result']

    def test_can_handle_df_list(self, test_client, small_data_post):
        small_data_post(random=True, wait=1)
        small_data_post(random=True, wait=1)
        small_data_post(random=True, wait=1)
        with test_client.session_transaction() as sess:
            data_tasks = sess['data_tasks']
            assert len(data_tasks) == 3
        df_list = ['${}$'.format(data_task_id) for data_task_id in data_tasks]
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='merge_df_task',
            args={'df_list': df_list}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'SUCCESS', new_body
        assert len(json.loads(json.loads(new_body['result'])['df'])) == 30

    def test_can_handle_empty_df_list(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            task_name='merge_df_task',
            args={'df_list': []}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['task_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'SUCCESS', new_body

"""This module tests the analytics controller module."""

import json
import time
import uuid
from uuid import uuid4

import flask
import pytest

from fractalis import sync, redis


class TestAnalytics:

    @pytest.fixture(scope='function')
    def test_client(self):
        sync.cleanup_all()
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            # cleanup running jobs after each test
            for job_id in flask.session['analytics_jobs']:
                test_client.delete('/analytics/{}?wait=1'.format(job_id))
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
            job_name='add_test_job',
            args={'a': 1, 'b': 2}
        )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        new_url = '/analytics/{}'.format(body['job_id'])
        assert uuid.UUID(body['job_id'])
        assert test_client.head(new_url).status_code == 200

    @pytest.fixture(scope='function',
                    params=[{'job_name': 'i_dont_exist_job',
                             'args': {'a': 1, 'b': 2}},
                            {'job_name': '',
                             'args': {'a': 1, 'b': 2}}])
    def bad_request(self, test_client, request):
        return test_client.post('/analytics',
                                data=flask.json.dumps(request.param))

    def test_400_if_POST_body_invalid(self, bad_request):
        assert bad_request.status_code == 400

    def test_no_conflict_when_running_job_twice(self, test_client):
        rv1 = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv1.status_code == 201

        rv2 = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv2.status_code == 201

        body1 = flask.json.loads(rv1.get_data())
        new_url1 = '/analytics/{}?wait=0'.format(body1['job_id'])
        new_response1 = test_client.get(new_url1)
        assert new_response1.status_code == 200
        new_body1 = flask.json.loads(new_response1.get_data())
        assert new_body1['state'] != 'FAILURE'

        body2 = flask.json.loads(rv2.get_data())
        new_url2 = '/analytics/{}?wait=0'.format(body2['job_id'])
        new_response2 = test_client.get(new_url2)
        assert new_response2.status_code == 200
        new_body2 = flask.json.loads(new_response2.get_data())
        assert new_body2['state'] != 'FAILURE'

    def test_404_if_no_session_auth(self, test_client, small_data_post):
        rv = small_data_post(random=False)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        with test_client.session_transaction() as sess:
            sess['data_ids'] = []
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='sum_df_test_job',
            args={'a': '${}$'.format(body['data_ids'][0])}
        )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        url = '/analytics/{}?wait=1'.format(body['job_id'])
        rv = test_client.get(url)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 200, body
        assert body['state'] == 'FAILURE', body
        assert 'KeyError' in body['result'], body

    def test_403_if_no_data_access_auth(self, test_client):
        rv = test_client.post(
            '/data?wait=1', data=flask.json.dumps(dict(
                handler='test',
                server='localhost:1234',
                auth={'token': '7746391376142672192764'},
                descriptors=[
                    {
                        'data_type': 'default',
                        'concept': 'concept'
                    }
                ]
            )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        assert len(body['data_ids']) == 1
        data_id = body['data_ids'][0]
        assert redis.get('data:{}'.format(data_id))
        with test_client.session_transaction() as sess:
            sess.sid = str(uuid4())  # we are someone else now
        rv = test_client.post(
            '/data?wait=1', data=flask.json.dumps(dict(
                handler='test',
                server='localhost:1234',
                auth={'token': 'fail'},  # we make the ETL fail on purpose
                descriptors=[
                    {
                        'data_type': 'default',
                        'concept': 'concept',
                    }
                ]
            )))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 201, body
        assert len(body['data_ids']) == 1
        assert body['data_ids'][0] == data_id
        rv = test_client.post('/analytics?wait=1', data=flask.json.dumps(dict(
            job_name='sum_df_test_job',
            args={'a': '${}$'.format(data_id)}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        url = '/analytics/{}?wait=1'.format(body['job_id'])
        rv = test_client.get(url)
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 200
        assert body['state'] == 'FAILURE'
        assert 'No permission' in body['result']

    def test_resource_deleted(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='add_test_job',
            args={'a': 1, 'b': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        assert test_client.head(new_url).status_code == 200
        assert test_client.delete(new_url).status_code == 200
        assert test_client.head(new_url).status_code == 404

    def test_404_if_deleting_non_existing_resource(self, test_client):
        rv = test_client.delete('/analytics/{}?wait=1'.format(str(uuid4())))
        assert rv.status_code == 404

    def test_running_resource_deleted(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        assert test_client.head(new_url).status_code == 200
        assert test_client.delete(new_url).status_code == 200
        assert test_client.head(new_url).status_code == 404

    def test_404_if_deleting_without_auth(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        time.sleep(1)
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        with test_client.session_transaction() as sess:
            sess['analytics_jobs'] = []
        assert test_client.delete(new_url).status_code == 404

    def test_status_contains_result_if_finished(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='add_test_job',
            args={'a': 1, 'b': 2}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'SUCCESS', new_body
        assert json.loads(new_body['result'])['sum'] == 3, new_body

    def test_status_result_empty_if_not_finished(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        time.sleep(1)
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=0'.format(body['job_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert not new_body['result']
        assert new_body['state'] == 'PENDING'

    def test_correct_response_if_job_fails(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='div_test_job',
            args={'a': 2, 'b': 0}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'FAILURE'
        assert 'ZeroDivisionError' in new_body['result']

    def test_404_if_status_non_existing_resource(self, test_client):
        assert test_client.get('/analytics/{}?wait=1'
                               .format(str(uuid4()))).status_code == 404

    def test_404_if_status_without_auth(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=0'.format(body['job_id'])
        with test_client.session_transaction() as sess:
            sess['analytics_jobs'] = []
        assert test_client.get(new_url).status_code == 404

    def test_float_when_summing_up_df(self, test_client, small_data_post):
        data_ids = []

        data_rv1 = small_data_post(random=True, wait=1)
        data_body1 = flask.json.loads(data_rv1.get_data())
        assert data_rv1.status_code == 201, data_body1
        data_ids += data_body1['data_ids']

        assert len(data_ids) == 1

        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='sum_df_test_job',
            args={'a': '${}$'.format(data_ids[0])}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = test_client.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_response.status_code == 200, new_body
        assert new_body['state'] == 'SUCCESS', new_body
        assert float(json.loads(new_body['result'])['sum'])

    def test_exception_if_result_not_json(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='invalid_json_job',
            args={'a': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'FAILURE'
        assert 'TypeError' in new_body['result']

    def test_exception_if_result_not_dict(self, test_client):
        rv = test_client.post('/analytics', data=flask.json.dumps(dict(
            job_name='no_dict_job',
            args={'a': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = test_client.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['state'] == 'FAILURE'
        assert 'TypeError' in new_body['result']

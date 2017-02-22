"""This module tests the analytics controller module."""

import uuid
import time

import flask
import pytest


class TestAnalytics(object):

    @pytest.fixture(scope='function')
    def app(self):
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            # cleanup running jobs after each test
            for job_id in flask.session['jobs']:
                test_client.delete('/analytics/{}?wait=1'.format(job_id))

    # test POST to /analytics

    def test_new_resource_created(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='add_test_job',
            args={'a': 1, 'b': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        assert uuid.UUID(body['job_id'])
        assert app.head(new_url).status_code == 200

    @pytest.fixture(scope='function',
                    params=[{'job_name': 'i_dont_exist_job',
                             'args': {'a': 1, 'b': 2}},
                            {'job_name': '',
                             'args': {'a': 1, 'b': 2}}])
    def bad_request(self, app, request):
        return app.post('/analytics', data=flask.json.dumps(request.param))

    def test_400_if_POST_body_invalid(self, bad_request):
        assert bad_request.status_code == 400

    def test_no_conflict_when_running_job_twice(self, app):
        rv1 = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv1.status_code == 201

        rv2 = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv2.status_code == 201

        body1 = flask.json.loads(rv1.get_data())
        new_url1 = '/analytics/{}?wait=0'.format(body1['job_id'])
        new_response1 = app.get(new_url1)
        assert new_response1.status_code == 200
        new_body1 = flask.json.loads(new_response1.get_data())
        new_body1['status'] != 'FAILURE'

        body2 = flask.json.loads(rv2.get_data())
        new_url2 = '/analytics/{}?wait=0'.format(body2['job_id'])
        new_response2 = app.get(new_url2)
        assert new_response2.status_code == 200
        new_body2 = flask.json.loads(new_response2.get_data())
        new_body2['status'] != 'FAILURE'

    @pytest.mark.skip(reason="Data interface not implemented yet.")
    def test_404_if_creating_without_auth(self, app):
        pass

    # test DELETE to /analytics/{job_id}

    def test_resource_deleted(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='add_test_job',
            args={'a': 1, 'b': 1}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        assert app.head(new_url).status_code == 200
        assert app.delete(new_url).status_code == 200
        assert app.head(new_url).status_code == 404

    def test_404_if_deleting_non_existing_resource(self, app):
        rv = app.delete('/analytics/{}?wait=1'.format(uuid.uuid4()))
        assert rv.status_code == 404

    def test_running_resource_deleted(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        assert app.head(new_url).status_code == 200
        assert app.delete(new_url).status_code == 200
        assert app.head(new_url).status_code == 404

    def test_404_if_deleting_without_auth(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        time.sleep(1)
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        with app.session_transaction() as sess:
            sess['jobs'] = []
        assert app.delete(new_url).status_code == 404

    # test GET to /analytics/{job_id}

    def test_status_contains_result_if_finished(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='add_test_job',
            args={'a': 1, 'b': 2}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = app.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['status'] == 'SUCCESS', new_body
        assert new_body['result'] == 3, new_body

    def test_status_result_empty_if_not_finished(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        time.sleep(1)
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=0'.format(body['job_id'])
        new_response = app.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert not new_body['result']
        assert new_body['status'] == 'PENDING'

    def test_correct_response_if_job_fails(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='div_test_job',
            args={'a': 2, 'b': 0}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=1'.format(body['job_id'])
        new_response = app.get(new_url)
        assert new_response.status_code == 200
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['status'] == 'FAILURE'
        assert 'ZeroDivisionError' in new_body['result']

    def test_404_if_status_non_existing_resource(self, app):
        assert app.get('/analytics/{}?wait=1'.format(uuid.uuid4())
                       ).status_code == 404

    def test_404_if_status_without_auth(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            job_name='nothing_test_job',
            args={'seconds': 4}
        )))
        assert rv.status_code == 201
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}?wait=0'.format(body['job_id'])
        with app.session_transaction() as sess:
            sess['jobs'] = []
        assert app.get(new_url).status_code == 404

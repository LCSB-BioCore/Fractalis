import uuid

import flask
import pytest


class TestAnalytics(object):

    @pytest.fixture(scope='module')
    def app(self):
        from flask import Flask
        app = Flask('test_app')
        app.testing = True
        test_client = app.test_client()
        return test_client

    # test POST to /analytics

    def test_new_resource_created(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.add',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert response.status_code == 201
        assert uuid.UUID(response_body['job_id'])
        assert app.head(new_resource_url).status_code == 200

    def test_400_if_POST_body_invalid_1(self, app):
        assert False

    def test_400_if_POST_body_invalid_2(self, app):
        assert False

    def test_400_if_POST_body_invalid_3(self, app):
        assert False

    def test_400_if_POST_body_invalid_4(self, app):
        assert False

    def test_400_if_creating_but_script_does_not_exist_1(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.querty',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        assert response.status_code == 400
        assert 'ImportError' in response_body['result']

    def test_400_if_creating_but_script_does_not_exist_2(self, app):
        response = app.post('/analytics', data=dict(
            script='querty.sample.add',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        assert response.status_code == 400
        assert 'ImportError' in response_body['result']

    def test_400_if_creating_but_arguments_are_invalid(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.add',
            arguments={'a': 1, 'c': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        assert response.status_code == 400
        assert 'TypeError' in response_body['result']

    def test_403_if_creating_but_not_authenticated(self, app):
        assert False

    # test DELETE to /analytics/{job_id}

    def test_resource_deleted(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.add',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert app.head(new_resource_url).status_code == 200
        assert app.delete(new_resource_url).status_code == 200
        assert app.head(new_resource_url).status_code == 404

    def test_404_if_deleting_non_existing_resource(self, app):
        response = app.delete('/analytics/{}'.format(uuid.uuid4()))
        assert response.status_code == 404

    def test_running_resource_deleted(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.do_nothing',
            arguments={'time': 10}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert app.head(new_resource_url).status_code == 200
        assert app.delete(new_resource_url).status_code == 200
        assert app.head(new_resource_url).status_code == 404

    def test_403_if_deleting_but_not_authenticated(self, app):
        assert False

    # test GET to /analytics/{job_id}

    def test_status_contains_result_if_finished(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.add',
            arguments={'a': 1, 'b': 2}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        new_response = app.get(new_resource_url)
        new_response_body = flask.json.loads(new_response.get_data())
        assert new_response_body['result'] == 3

    def test_status_result_empty_if_not_finished(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.do_nothing',
            arguments={'time': 10}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        new_response = app.get(new_resource_url)
        new_response_body = flask.json.loads(new_response.get_data())
        assert not new_response_body['result']
        assert new_response_body['status'] == 'RUNNING'

    def test_correct_response_if_task_fails(self, app):
        response = app.post('/analytics', data=dict(
            script='test.sample.div',
            arguments={}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        new_response = app.get(new_resource_url)
        new_response_body = flask.json.loads(new_response.get_data())
        assert new_response_body['status'] == 'FAILURE'
        assert 'ZeroDivisionError' in new_response_body['result']

    def test_404_if_status_non_existing_resource(self, app):
        assert app.get('/analytics/{}'.format(uuid.uuid4())).status_code == 404

    def test_403_when_getting_status_but_not_authenticated(self, app):
        assert False

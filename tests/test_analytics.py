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

    def test_new_resource_created(self, app):
        response = app.post('/analytics', data=dict(
            script='test/sample.py',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert response.status_code == 201
        assert uuid.UUID(response_body['job_id'])
        assert app.head(new_resource_url).status_code == 200

    def test_400_if_creating_but_script_does_not_exist(self, app):
        response = app.post('/analytics', data=dict(
            script='test/sapmle.py',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        assert response.status_code == 400
        assert response_body['error_msg']

    def test_400_if_creating_but_arguments_are_invalid(self, app):
        response = app.post('/analytics', data=dict(
            script='test/sample.py',
            arguments={'a': 1, 'c': 1}
        ))
        response_body = flask.json.loads(response.get_data())
        assert response.status_code == 400
        assert response_body['error_msg']

    def test_403_if_creating_but_not_authenticated(self, app):
        response = app.post('/analytics', data=dict(
            script='test/sample.py',
            arguments={'a': 1, 'b': 1}
        ))
        assert response.status_code == 403

    def test_resource_deleted(self, app):
        response = app.post('/analytics')
        response_body = flask.json.loads(response.get_data())
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert app.delete(new_resource_url).status_code == 200
        assert app.head(new_resource_url).status_code == 404

    def test_403_if_deleting_but_not_authenticated(self, app):
        assert False

    def test_404_if_deleting_non_existing_resource(self, app):
        assert False

    def test_403_when_getting_status_but_not_authenticated(self, app):
        assert False

    def test_status_result_non_empty_if_finished(self, app):
        assert False

    def test_status_result_empty_if_not_finished(self, app):
        assert False

    def test_404_if_status_non_existing_resource(self, app):
        assert False

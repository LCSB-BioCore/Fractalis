import uuid
import json

import pytest


class TestAnalytics(object):

    @pytest.fixture
    def flask_app(self):
        from flask import Flask
        from fractalis.config import configure_app
        app = Flask('test_app')
        configure_app(app, mode='testing')
        test_client = app.test_client()
        return test_client

    def test_new_resource_created(self, flask_app):
        response = flask_app.post('/analytics', data=dict(
            script='test/sample.py',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = json.loads(response.get_data().decode('utf-8'))
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert response.status_code == 201
        assert uuid.UUID(response_body['job_id'])
        assert flask_app.head(new_resource_url).status_code == 200

    def test_400_if_creating_but_script_does_not_exist(self, flask_app):
        response = flask_app.post('/analytics', data=dict(
            script='test/sapmle.py',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = json.loads(response.get_data().decode('utf-8'))
        assert response.status_code == 400
        assert response_body['error_msg']

    def test_400_if_creating_but_arguments_are_invalid(self, flask_app):
        response = flask_app.post('/analytics', data=dict(
            script='test/sample.py',
            arguments={'a': 1, 'c': 1}
        ))
        response_body = json.loads(response.get_data().decode('utf-8'))
        assert response.status_code == 400
        assert response_body['error_msg']

    def test_403_if_creating_but_not_authenticated(self, flask_app):
        response = flask_app.post('/analytics', data=dict(
            script='test/sample.py',
            arguments={'a': 1, 'b': 1}
        ))
        response_body = json.loads(response.get_data().decode('utf-8'))
        assert response.status_code == 403

    def test_resource_deleted(self, flask_app):
        response = flask_app.post('/analytics')
        response_body = json.loads(response.get_data().decode('utf-8'))
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert flask_app.delete(new_resource_url).status_code == 200
        assert flask_app.head(new_resource_url).status_code == 404

    def test_403_if_deleting_but_not_authenticated(self, flask_app):
        assert False

    def test_404_if_deleting_non_existing_resource(self, flask_app):
        assert False

    def test_403_when_getting_status_but_not_authenticated(self, flask_app):
        assert False

    def test_status_result_non_empty_if_finished(self, flask_app):
        assert False

    def test_status_result_empty_if_not_finished(self, flask_app):
        assert False

    def test_404_if_status_non_existing_resource(self, flask_app):
        assert False

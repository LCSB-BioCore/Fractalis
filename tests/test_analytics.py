import uuid
import json

import pytest


class TestAnalytics(object):

    @pytest.fixture
    def test_client(self):
        from flask import Flask
        from fractalis.config import configure_app
        app = Flask('test_app')
        configure_app(app, mode='testing')
        test_client = app.test_client()
        return test_client

    def test_create_new_resource(self, test_client):
        response = test_client.post('/analytics')
        response_body = json.loads(response.get_data().decode('utf-8'))
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert response.status_code == 201
        assert uuid.UUID(response_body['job_id'])
        assert test_client.head(new_resource_url).status_code == 200

    def test_create_new_resource_with_not_existing_script(self, test_client):
        assert False

    def test_create_new_resource_not_authenticated(self, test_client):
        assert False

    def test_delete_resource(self, test_client):
        response = test_client.post('/analytics')
        response_body = json.loads(response.get_data().decode('utf-8'))
        new_resource_url = '/analytics/{}'.format(response_body['job_id'])
        assert test_client.delete(new_resource_url).status_code == 200
        assert test_client.head(new_resource_url).status_code == 404

    def test_delete_resource_not_authenticated(self, test_client):
        assert False

    def test_delete_not_existing_resource(self, test_client):
        assert False

    def test_get_status_running(self, test_client):
        assert False

    def test_get_status_not_authenticated(self, test_client):
        assert False

    def test_get_status_if_script_failure(self, test_client):
        assert False

    def test_get_status_if_script_success(self, test_client):
        assert False

    def test_get_status_for_non_existing_resource(self, test_client):
        assert False

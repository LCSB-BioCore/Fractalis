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
        rv = app.post('/analytics', data=dict(
            task='test.sample.add',
            arguments={'a': 1, 'b': 1}
        ))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        assert rv.status_code == 201
        assert uuid.UUID(body['job_id'])
        assert app.head(new_url).status_code == 200

    def test_400_if_POST_body_invalid_1(self, app):
        rv = app.post('/analytics', data=dict(
            arguments={'a': 1, 'b': 1}
        ))
        assert rv.status_code == 400

    def test_400_if_POST_body_invalid_2(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.add'
        ))
        assert rv.status_code == 400

    def test_400_if_POST_body_invalid_3(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.add',
            arguments='something'
        ))
        assert rv.status_code == 400

    def test_400_if_POST_body_invalid_4(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.add',
            arguments={}
        ))
        assert rv.status_code == 400

    def test_400_if_POST_body_invalid_5(self, app):
        rv = app.post('/analytics', data=dict(
            task='',
            arguments={'a': 1, 'b': 1}
        ))
        assert rv.status_code == 400

    def test_400_if_creating_but_task_does_not_exist_1(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.querty',
            arguments={'a': 1, 'b': 1}
        ))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 400
        assert 'ImportError' in body['result']

    def test_400_if_creating_but_task_does_not_exist_2(self, app):
        rv = app.post('/analytics', data=dict(
            task='querty.sample.add',
            arguments={'a': 1, 'b': 1}
        ))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 400
        assert 'ImportError' in body['result']

    def test_400_if_creating_but_arguments_are_invalid(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.add',
            arguments={'a': 1, 'c': 1}
        ))
        body = flask.json.loads(rv.get_data())
        assert rv.status_code == 400
        assert 'TypeError' in body['result']

    def test_403_if_creating_but_not_authenticated(self, app):
        assert False

    # test DELETE to /analytics/{job_id}

    def test_resource_deleted(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.add',
            arguments={'a': 1, 'b': 1}
        ))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        assert app.head(new_url).status_code == 200
        assert app.delete(new_url).status_code == 200
        assert app.head(new_url).status_code == 404

    def test_404_if_deleting_non_existing_resource(self, app):
        rv = app.delete('/analytics/{}'.format(uuid.uuid4()))
        assert rv.status_code == 404

    def test_running_resource_deleted(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.do_nothing',
            arguments={'time': 10}
        ))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        assert app.head(new_url).status_code == 200
        assert app.delete(new_url).status_code == 200
        assert app.head(new_url).status_code == 404

    def test_403_if_deleting_but_not_authenticated(self, app):
        assert False

    # test GET to /analytics/{job_id}

    def test_status_contains_result_if_finished(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.add',
            arguments={'a': 1, 'b': 2}
        ))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        new_response = app.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['result'] == 3

    def test_status_result_empty_if_not_finished(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.do_nothing',
            arguments={'time': 10}
        ))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        new_response = app.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert not new_body['result']
        assert new_body['status'] == 'RUNNING'

    def test_correct_response_if_task_fails(self, app):
        rv = app.post('/analytics', data=dict(
            task='test.sample.div',
            arguments={}
        ))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['job_id'])
        new_response = app.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['status'] == 'FAILURE'
        assert 'ZeroDivisionError' in new_body['result']

    def test_404_if_status_non_existing_resource(self, app):
        assert app.get('/analytics/{}'.format(uuid.uuid4())).status_code == 404

    def test_403_when_getting_status_but_not_authenticated(self, app):
        assert False

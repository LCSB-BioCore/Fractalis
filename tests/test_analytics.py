import uuid

import flask
import pytest


class TestAnalytics(object):

    @pytest.fixture(scope='module')
    def app(self):
        from fractalis import app
        app.testing = True
        test_client = app.test_client()
        return test_client

    # test POST to /analytics

    def test_new_resource_created(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            task='test.add',
            args={'a': 1, 'b': 1}
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['task_id'])
        assert rv.status_code == 201
        assert uuid.UUID(body['task_id'])
        assert app.head(new_url).status_code == 200

    @pytest.fixture(scope='module',
                    params=[{'task': 'querty.add',
                             'args': {'a': 1, 'b': 2}},
                            {'task': 'test.querty',
                             'args': {'a': 1, 'b': 2}},
                            {'task': 'test.add',
                             'args': {'a': 1, 'c': 2}},
                            {'task': 'test.add',
                             'args': {'a': 1}},
                            {'task': 'test.add'},
                            {'args': {'a': 1, 'b': 2}},
                            {'task': '',
                             'args': {'a': 1, 'b': 2}},
                            {'task': 'querty.add',
                             'args': ''}])
    def bad_request(self, app, request):
        return app.post('/analytics', data=flask.json.dumps(request.param))

    def test_400_if_POST_body_invalid(self, bad_request):
        assert bad_request.status_code == 400

    def test_403_if_creating_but_not_authenticated(self, app):
        assert False

    # test DELETE to /analytics/{task_id}

    def test_resource_deleted(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            task='test.add',
            args={'a': 1, 'b': 1}
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['task_id'])
        assert app.head(new_url).status_code == 200
        assert app.delete(new_url).status_code == 200
        assert app.head(new_url).status_code == 404

    def test_404_if_deleting_non_existing_resource(self, app):
        rv = app.delete('/analytics/{}'.format(uuid.uuid4()))
        assert rv.status_code == 404

    def test_running_resource_deleted(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            task='test.do_nothing',
            args={'time': 10}
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['task_id'])
        assert app.head(new_url).status_code == 200
        assert app.delete(new_url).status_code == 200
        assert app.head(new_url).status_code == 404

    def test_403_if_deleting_but_not_authenticated(self, app):
        assert False

    # test GET to /analytics/{task_id}

    def test_status_contains_result_if_finished(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            task='test.add',
            args={'a': 1, 'b': 2}
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['task_id'])
        new_response = app.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['result'] == 3

    def test_status_result_empty_if_not_finished(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            task='test.do_nothing',
            args={'time': 10}
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['task_id'])
        new_response = app.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert not new_body['result']
        assert new_body['status'] == 'RUNNING'

    def test_correct_response_if_task_fails(self, app):
        rv = app.post('/analytics', data=flask.json.dumps(dict(
            task='test.div',
            args={}
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/analytics/{}'.format(body['task_id'])
        new_response = app.get(new_url)
        new_body = flask.json.loads(new_response.get_data())
        assert new_body['status'] == 'FAILURE'
        assert 'ZeroDivisionError' in new_body['result']

    def test_404_if_status_non_existing_resource(self, app):
        assert app.get('/analytics/{}'.format(uuid.uuid4())).status_code == 404

    def test_403_when_getting_status_but_not_authenticated(self, app):
        assert False

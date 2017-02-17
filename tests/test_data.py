"""This module tests the data controller module."""

import flask
import pytest


@pytest.mark.skip(reason='notimplemented')
class TestData(object):

    @pytest.fixture(scope='function')
    def app(self, app):
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client

    # POST /

    def test_201_on_POST_and_resource_exists_if_created(self, app):
        rv = app.post('/data', data=flask.json.dumps(dict(
            etl='transmart',
            server='localhost:1234',
            concept='GSE123/Demographics/Age',
        )))
        body = flask.json.loads(rv.get_data())
        new_url = '/data/{}'.format(body['data_id'])
        assert app.head(new_url) == 200

    def test_400_on_POST_if_invalid_request(self, app):
        assert False

    def test_200_instead_of_201_on_POST_if_data_already_exists(self, app):
        assert False

    def test_data_deleted_on_expiration(self, app):
        assert False

    def test_data_in_db_after_creation(self, app):
        assert False

    # GET /data_id

    def test_200_on_GET_if_resource_created_and_correct_content(self, app):
        assert False

    def test_400_on_GET_if_invalid_request(self, app):
        assert False

    def test_404_on_GET_if_dataid_not_existing(self, app):
        assert False

    def test_404_on_GET_if_no_auth(self, app):
        assert False

    # GET /

    def test_200_on_GET_and_correct_summary_if_data_exist(self, app):
        assert False

    def test_200_on_GET_and_correct_summary_if_no_data_exist(self, app):
        assert False

    def test_only_permitted_data_visible(self, app):
        assert False

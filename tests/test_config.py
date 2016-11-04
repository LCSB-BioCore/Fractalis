import os
from importlib import reload

import pytest

import fractalis


class TestConfig(object):

    def test_config_when_test_mode(self):
        os.environ['FRACTALIS_MODE'] = 'testing'
        reload(fractalis)
        assert not fractalis.flask_app.config['DEBUG']
        assert fractalis.flask_app.config['TESTING']

    def test_config_when_development_mode(self):
        os.environ['FRACTALIS_MODE'] = 'development'
        reload(fractalis)
        assert fractalis.flask_app.config['DEBUG']
        assert not fractalis.flask_app.config['TESTING']

    def test_config_when_production_mode(self):
        os.environ['FRACTALIS_MODE'] = 'production'
        reload(fractalis)
        assert not fractalis.flask_app.config['DEBUG']
        assert not fractalis.flask_app.config['TESTING']

    def test_config_when_default(self):
        del os.environ['FRACTALIS_MODE']
        reload(fractalis)
        assert not fractalis.flask_app.config['DEBUG']
        assert not fractalis.flask_app.config['TESTING']

    def test_config_when_unknown_mode(self):
        os.environ['FRACTALIS_MODE'] = 'foobar'
        with pytest.raises(KeyError):
            reload(fractalis)

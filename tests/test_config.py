import os
import fractalis
from importlib import reload

class TestConfig:

    def test_config_when_test_mode(self):
        os.environ['FRACTALIS_MODE'] = 'testing'
        reload(fractalis)
        assert not fractalis.app.config['DEBUG']
        assert fractalis.app.config['TESTING']

    def test_config_when_development_mode(self):
        os.environ['FRACTALIS_MODE'] = 'development'
        reload(fractalis)
        assert fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

    def test_config_when_production_mode(self):
        os.environ['FRACTALIS_MODE'] = 'production'
        reload(fractalis)
        assert not fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

    def test_config_when_default(self):
        del os.environ['FRACTALIS_MODE']
        reload(fractalis)
        assert not fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

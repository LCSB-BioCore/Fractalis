import unittest
import fractalis
from importlib import reload
from test.support import EnvironmentVarGuard

class FractalisConfigTestCase(unittest.TestCase):

    env_var_guard = EnvironmentVarGuard()

    def test_config_when_test_mode(self):
        self.env_var_guard.set('FRACTALIS_MODE', 'testing')
        reload(fractalis)
        assert not fractalis.app.config['DEBUG']
        assert fractalis.app.config['TESTING']

    def test_config_when_development_mode(self):
        self.env_var_guard.set('FRACTALIS_MODE', 'development')
        reload(fractalis)
        assert fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

    def test_config_when_production_mode(self):
        self.env_var_guard.set('FRACTALIS_MODE', 'production')
        reload(fractalis)
        assert not fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

    def test_config_when_default(self):
        self.env_var_guard.unset('FRACTALIS_MODE')
        reload(fractalis)
        assert not fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

if __name__ == '__main__':
    unittest.main()

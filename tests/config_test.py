import unittest
import fractalis
from test.support import EnvironmentVarGuard

class FractalisConfigTestCase(unittest.TestCase):

    def test_config_when_test_mode(self):
        EnvironmentVarGuard().set('FRACTALIS_MODE', 'testing')
        fractalis.__init__('test_app')
        assert not fractalis.app.config['DEBUG']
        assert fractalis.app.config['TESTING']

    def test_config_when_development_mode(self):
        EnvironmentVarGuard().set('FRACTALIS_MODE', 'development')
        fractalis.__init__('test_app')
        assert fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

    def test_config_when_production_mode(self):
        EnvironmentVarGuard().set('FRACTALIS_MODE', 'production')
        fractalis.__init__('test_app')
        assert not fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

    def test_config_when_default(self):
        EnvironmentVarGuard().unset('FRACTALIS_MODE')
        fractalis.__init__('test_app')
        assert not fractalis.app.config['DEBUG']
        assert not fractalis.app.config['TESTING']

if __name__ == '__main__':
    unittest.main()

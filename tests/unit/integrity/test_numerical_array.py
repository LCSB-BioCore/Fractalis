"""This module provides tests for the integrity checker
for the 'numerical_array' data type."""

import pandas as pd
import pytest

from fractalis.data.check import IntegrityCheck


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestNumericalArrayIntegrityCheck:

    checker = IntegrityCheck.factory('numerical_array')

    def test_correct_check_1(self):
        df = pd.DataFrame([['1', '2', 3]], columns=['id', 'feature', 'value'])
        self.checker.check(df)

    def test_correct_check_2(self):
        df = pd.DataFrame([['1', '2', 3]], columns=['id', 'feat', 'value'])
        with pytest.raises(ValueError) as e:
            self.checker.check(df)
            assert 'must contain the columns' in e

    def test_correct_check_3(self):
        df = pd.DataFrame([[1, '2', 3]], columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.checker.check(df)
            assert "must be of type 'object'" in e

    def test_correct_check_4(self):
        df = pd.DataFrame([['1', 2, 3]], columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.checker.check(df)
            assert "must be of type 'object'" in e

    def test_correct_check_5(self):
        df = pd.DataFrame([['1', '2', '3']],
                          columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.checker.check(df)
            assert "must be of type 'np.int'" in e

    def test_correct_check_6(self):
        df = pd.DataFrame([['1', '2', 3], ['4', '2', 3]],
                          columns=['id', 'feature', 'value'])
        self.checker.check(df)

    def test_correct_check_7(self):
        df = pd.DataFrame([['1', '2', 3], ['4', '4', 3]],
                          columns=['id', 'feature', 'value'])
        self.checker.check(df)

    def test_correct_check_8(self):
        df = pd.DataFrame([['1', '2', 3], ['1', '4', 3], ['1', '2', 3]],
                          columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.checker.check(df)
            assert "must be unique" in e

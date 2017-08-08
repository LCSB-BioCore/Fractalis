"""This module provides tests for the array_utils module."""

import pytest
import pandas as pd

from fractalis.analytics.tasks.shared import array_utils


# noinspection PyMissingOrEmptyDocstring,PyMethodMayBeStatic
class TestArrayUtils:

    def test_drop_ungrouped_samples_1(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[]]
        with pytest.raises(ValueError) as e:
            array_utils.drop_ungrouped_samples(df=df, subsets=subsets)
            assert 'must not be empty' in e

    def test_drop_ungrouped_samples_2(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], [3]]
        df = array_utils.drop_ungrouped_samples(df=df, subsets=subsets)
        assert [0, 1, 3] == list(df)

    def test_drop_ungrouped_samples_3(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], []]
        df = array_utils.drop_ungrouped_samples(df=df, subsets=subsets)
        assert [0, 1] == list(df)

    def test_drop_ungrouped_samples_4(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], [5]]
        df = array_utils.drop_ungrouped_samples(df=df, subsets=subsets)
        assert [0, 1] == list(df)

    def test_drop_ungrouped_samples_5(self):
        df = pd.DataFrame()
        subsets = [[0, 1], [5]]
        df = array_utils.drop_ungrouped_samples(df=df, subsets=subsets)
        assert not list(df)

    def test_drop_unused_subset_ids_1(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = []
        subsets = array_utils.drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == []

    def test_drop_unused_subset_ids_2(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[]]
        subsets = array_utils.drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[]]

    def test_drop_unused_subset_ids_3(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], [1, 2, 4], [8]]
        subsets = array_utils.drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[0, 1], [1, 2], []]

    def test_drop_unused_subset_ids_4(self):
        df = pd.DataFrame()
        subsets = [[0, 1]]
        subsets = array_utils.drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[]]

    def test_melt_standard_format_df_works_for_standard_df(self):
        df = pd.DataFrame([['foo', 5, 10],
                           ['bar', 10, 15]],
                          columns=['variable', 101, 102])
        df = array_utils.melt_standard_format_df(df)
        assert list(df) == ['id', 'variable', 'value']
        assert df.shape == (4, 3)

    def test_melt_standard_format_df_works_for_minimal_df(self):
        df = pd.DataFrame([['foo', 5]], columns=['variable', 101])
        df = array_utils.melt_standard_format_df(df)
        assert list(df) == ['id', 'variable', 'value']
        assert df.shape == (1, 3)

    def test_melt_standard_format_df_raises_for_invalid_df(self):
        df = pd.DataFrame([['foo']], columns=['variable'])
        with pytest.raises(ValueError) as e:
            array_utils.melt_standard_format_df(df)
            assert 'must be non-empty' in e

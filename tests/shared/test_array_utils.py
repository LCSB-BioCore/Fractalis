"""This module provides tests for the array_utils module."""

import pytest
import pandas as pd

from fractalis.analytics.tasks.shared.array_utils \
    import drop_unused_subset_ids, drop_ungrouped_samples


# noinspection PyMissingOrEmptyDocstring,PyMethodMayBeStatic
class TestArrayUtils:

    def test_drop_ungrouped_samples_1(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[]]
        with pytest.raises(ValueError) as e:
            drop_ungrouped_samples(df=df, subsets=subsets)
            assert 'must not be empty' in e

    def test_drop_ungrouped_samples_2(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], [3]]
        df = drop_ungrouped_samples(df=df, subsets=subsets)
        assert [0, 1, 3] == list(df)

    def test_drop_ungrouped_samples_3(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], []]
        df = drop_ungrouped_samples(df=df, subsets=subsets)
        assert [0, 1] == list(df)

    def test_drop_ungrouped_samples_4(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], [5]]
        df = drop_ungrouped_samples(df=df, subsets=subsets)
        assert [0, 1] == list(df)

    def test_drop_ungrouped_samples_5(self):
        df = pd.DataFrame()
        subsets = [[0, 1], [5]]
        df = drop_ungrouped_samples(df=df, subsets=subsets)
        assert not list(df)

    def test_drop_unused_subset_ids_1(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = []
        subsets = drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == []

    def test_drop_unused_subset_ids_2(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[]]
        subsets = drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[]]

    def test_drop_unused_subset_ids_3(self):
        df = pd.DataFrame([[5, 10, 15, 20]])
        subsets = [[0, 1], [1, 2, 4], [8]]
        subsets = drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[0, 1], [1, 2], []]

    def test_drop_unused_subset_ids_4(self):
        df = pd.DataFrame()
        subsets = [[0, 1]]
        subsets = drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[]]

"""This module provides tests for the heatmap analysis main module."""

import pytest
import pandas as pd

from fractalis.analytics.tasks.heatmap.main import HeatmapTask


# noinspection PyMissingTypeHints
class TestHeatmap:

    task = HeatmapTask()

    def test_functional_1(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6], [102, 'foo', 10],
                          [102, 'bar', 11], [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[101, 102], [103, 104]]
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='B',
                                id_filter=[],
                                subsets=subsets)
        assert 'data' in result
        assert 'stats' in result

    def test_main_raises_if_invalid_data(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6], [102, 'foo', 10],
                          [102, 'bar', 11], [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[1, 2, 3, 4]]  # does not match sample colnames
        with pytest.raises(ValueError) as e:
            self.task.main(numerical_arrays=numerical_arrays,
                           numericals=[],
                           categoricals=[],
                           ranking_method='mean',
                           id_filter=[],
                           subsets=subsets)
            assert 'subset sample ids do not match the data' in e

    def test_main_raises_if_invalid_subsets(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6], [102, 'foo', 10],
                          [102, 'bar', 11], [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[101, 102, 103], [123]]
        with pytest.raises(ValueError) as e:
            self.task.main(numerical_arrays=numerical_arrays,
                           numericals=[],
                           categoricals=[],
                           ranking_method='mean',
                           id_filter=[],
                           subsets=subsets)
            assert 'specified subsets does not match' in e

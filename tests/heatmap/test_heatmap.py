"""This module provides tests for the heatmap analysis main module."""

import pytest
import pandas as pd

from fractalis.analytics.tasks.heatmap.main import HeatmapTask


# noinspection PyMissingTypeHints
class TestHeatmap:

    task = HeatmapTask()

    def test_melt_standard_format_df_works_for_standard_df(self):
        df = pd.DataFrame([['foo', 5, 10],
                           ['bar', 10, 15]],
                          columns=['variable', 101, 102])
        df = self.task.melt_standard_format_df(df)
        assert list(df) == ['id', 'variable', 'value']
        assert df.shape == (4, 3)

    def test_melt_standard_format_df_works_for_minimal_df(self):
        df = pd.DataFrame([['foo', 5]], columns=['variable', 101])
        df = self.task.melt_standard_format_df(df)
        assert list(df) == ['id', 'variable', 'value']
        assert df.shape == (1, 3)

    def test_melt_standard_format_df_raises_for_invalid_df(self):
        df = pd.DataFrame([['foo']], columns=['variable'])
        with pytest.raises(ValueError) as e:
            self.task.melt_standard_format_df(df)
            assert 'must be non-empty' in e

    def test_get_limma_stats_raises_for_invalid_subsets(self):
        df = pd.DataFrame([['foo', 5, 10, 15, 20]],
                          columns=['variable', 0, 1, 2, 3])
        subsets = [[0, 1]]
        with pytest.raises(ValueError) as e:
            self.task.get_limma_stats(df=df, subsets=subsets)
            assert 'requires at least two' in e

    def test_get_limma_stats_raises_for_invalid_df(self):
        df = pd.DataFrame([['foo']], columns=['variable'])
        subsets = [[0], [0]]
        with pytest.raises(ValueError) as e:
            self.task.get_limma_stats(df=df, subsets=subsets)
            assert 'dimension 1x2 or more' in e

    def test_get_limma_stats_returns_correct_for_2_groups(self):
        df = pd.DataFrame([['foo', 5, 10, 15, 20]],
                          columns=['variable', 0, 1, 2, 3])
        subsets = [[0, 1], [2, 3]]
        stats = self.task.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['variable', 'logFC', 'AveExpr', 't', 'P.Value', 'adj.P.Val', 'B'])

    def test_get_limma_stats_returns_correct_for_3_groups(self):
        df = pd.DataFrame([['foo', 5, 10, 15, 20]],
                          columns=['variable', 0, 1, 2, 3])
        subsets = [[0, 1], [2], [3]]
        stats = self.task.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['variable', 'AveExpr', 'F', 'P.Value', 'adj.P.Val'])
        assert all(stat not in list(stats) for stat in ['logFC', 'B', 't'])

    def test_get_limma_stats_returns_correct_for_4_groups(self):
        df = pd.DataFrame([['foo', 5, 10, 15, 20]],
                          columns=['variable', 0, 1, 2, 3])
        subsets = [[0, 1], [1, 2], [2, 3], [3, 0]]
        stats = self.task.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['variable', 'AveExpr', 'F', 'P.Value', 'adj.P.Val'])
        assert all(stat not in list(stats) for stat in ['logFC', 'B', 't'])

    def test_functional_1(self):
        numerical_arrays = [
            pd.DataFrame([['foo', 5, 10, 15, 20], ['bar', 6, 11, 16, 21]],
                         columns=['variable', 101, 102, 103, 104])
        ]
        subsets = [[101, 102], [103, 104]]
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                subsets=subsets)
        assert 'data' in result
        assert 'stats' in result

    def test_main_raises_if_invalid_data(self):
        numerical_arrays = [
            pd.DataFrame([['foo', 5, 10, 15, 20]],
                         columns=['variable', 101, 102, 103, 104])
        ]
        subsets = [[0, 1, 2, 3]]  # does not match sample colnames
        with pytest.raises(ValueError) as e:
            self.task.main(numerical_arrays=numerical_arrays,
                                    numericals=[],
                                    categoricals=[],
                                    subsets=subsets)
            assert 'subset sample ids do not match the data' in e

    def test_main_raises_if_invalid_subsets(self):
        numerical_arrays = [
            pd.DataFrame([['foo', 5, 10, 15, 20]],
                         columns=['variable', 101, 102, 103, 104])
        ]
        subsets = [[101, 102, 103], [123]]
        with pytest.raises(ValueError) as e:
            result = self.task.main(numerical_arrays=numerical_arrays,
                                    numericals=[],
                                    categoricals=[],
                                    subsets=subsets)
            assert 'specified subsets does not match' in e

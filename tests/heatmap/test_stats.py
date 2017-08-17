"""This module provides tests for the array_utils module."""

import pytest
import pandas as pd

from fractalis.analytics.tasks.heatmap.stats import StatisticTask


# noinspection PyMissingOrEmptyDocstring,PyMethodMayBeStatic,PyMissingTypeHints
class TestArrayUtils:

    stat_task = StatisticTask()

    def test_get_limma_stats_raises_for_invalid_subsets(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1]]
        with pytest.raises(ValueError) as e:
            self.stat_task.get_limma_stats(df=df, subsets=subsets)
            assert 'requires at least two' in e

    def test_get_limma_stats_raises_for_invalid_df(self):
        df = pd.DataFrame([], index=['foo'], columns=[])
        subsets = [[0], [0]]
        with pytest.raises(ValueError) as e:
            self.stat_task.get_limma_stats(df=df, subsets=subsets)
            assert 'dimension 1x2 or more' in e

    def test_get_limma_stats_returns_correct_for_2_groups(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [2, 3]]
        stats = self.stat_task.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['feature', 'logFC', 'AveExpr', 't', 'P.Value', 'adj.P.Val',
                    'B'])

    def test_get_limma_stats_returns_correct_for_3_groups(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [2], [3]]
        stats = self.stat_task.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['feature', 'AveExpr', 'F', 'P.Value', 'adj.P.Val'])
        assert all(stat not in list(stats) for stat in ['logFC', 'B', 't'])

    def test_get_limma_stats_returns_correct_for_4_groups(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [1, 2], [2, 3], [3, 0]]
        stats = self.stat_task.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['feature', 'AveExpr', 'F', 'P.Value', 'adj.P.Val'])
        assert all(stat not in list(stats) for stat in ['logFC', 'B', 't'])

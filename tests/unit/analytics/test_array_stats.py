"""This module provides tests for the array_stats module."""

import pytest
import pandas as pd


from fractalis.analytics.tasks.shared import array_stats


# noinspection PyMissingOrEmptyDocstring,PyMethodMayBeStatic,PyMissingTypeHints
class TestArrayStats:

    def test_get_limma_stats_raises_for_invalid_subsets(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1]]
        with pytest.raises(ValueError) as e:
            array_stats.get_limma_stats(df=df, subsets=subsets)
            assert 'requires at least two' in e

    def test_get_limma_stats_raises_for_invalid_df(self):
        df = pd.DataFrame([], index=['foo'], columns=[])
        subsets = [[0], [0]]
        with pytest.raises(ValueError) as e:
            array_stats.get_limma_stats(df=df, subsets=subsets)
            assert 'dimension 1x2 or more' in e

    def test_get_limma_stats_returns_correct_for_2_groups(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [2, 3]]
        stats = array_stats.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['feature', 'logFC', 'AveExpr', 't', 'P.Value', 'adj.P.Val',
                    'B'])

    def test_get_limma_stats_returns_correct_for_3_groups(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [2], [3]]
        stats = array_stats.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['feature', 'AveExpr', 'F', 'P.Value', 'adj.P.Val'])
        assert all(stat not in list(stats) for stat in ['logFC', 'B', 't'])

    def test_get_limma_stats_returns_correct_for_4_groups(self):
        df = pd.DataFrame([[5, 10, 15, 20]], index=['foo'],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [1, 2], [2, 3], [3, 0]]
        stats = array_stats.get_limma_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['feature', 'AveExpr', 'F', 'P.Value', 'adj.P.Val'])
        assert all(stat not in list(stats) for stat in ['logFC', 'B', 't'])

    def test_get_deseq2_stats_returns_correct_for_2_groups(self):
        df = pd.DataFrame([[500, 1, 1, 500],
                           [1, 500, 500, 1]],
                          columns=[0, 1, 2, 3])
        subsets = [[0, 1], [2, 3]]
        stats = array_stats.get_deseq2_stats(df=df, subsets=subsets)
        assert all(stat in list(stats) for stat in
                   ['baseMean', 'log2FoldChange', 'lfcSE',
                    'stat', 'pvalue', 'padj'])

    def test_deseq2_requires_exactly_2_subsets(self):
        with pytest.raises(ValueError):
            array_stats.get_deseq2_stats(df=pd.DataFrame(),
                                         subsets=[[], [], []])
        with pytest.raises(ValueError):
            array_stats.get_deseq2_stats(df=pd.DataFrame(), subsets=[[]])

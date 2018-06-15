"""This module provides tests for the heatmap analysis main module."""

import pytest
import pandas as pd
import numpy as np

from fractalis.analytics.tasks.heatmap.main import HeatmapTask


# noinspection PyMissingTypeHints
class TestHeatmap:

    task = HeatmapTask()

    def test_functional(self):
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
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        assert 'data' in result
        assert 'stats' in result

    def test_functional_with_nans_and_missing(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 5],
                          [102, 'foo', 10],
                          [103, 'foo', float('nan')], [103, 'bar', 15],
                          [104, 'foo', 20], [104, 'bar', 20]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[101, 102], [103, 104]]
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='B',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        for stat in result['stats']:
            if stat != 'feature' and stat != 'AveExpr':
                assert result['stats'][stat][0] == result['stats'][stat][1]

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
                           params={},
                           id_filter=[],
                           max_rows=100,
                           subsets=subsets)
            assert 'data set is too small' in e

    def test_empty_subset_equals_full_subset(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6], [102, 'foo', 10],
                          [102, 'bar', 11], [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value'])
        ]
        result_1 = self.task.main(numerical_arrays=numerical_arrays,
                                  numericals=[],
                                  categoricals=[],
                                  ranking_method='mean',
                                  params={},
                                  id_filter=[],
                                  max_rows=100,
                                  subsets=[])

        result_2 = self.task.main(numerical_arrays=numerical_arrays,
                                  numericals=[],
                                  categoricals=[],
                                  ranking_method='mean',
                                  params={},
                                  id_filter=[],
                                  max_rows=100,
                                  subsets=[[101, 102, 103, 104]])
        assert result_1 == result_2

    def test_multiple_numerical_array_data(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6],
                          [102, 'foo', 10], [102, 'bar', 11],
                          [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value']),
            pd.DataFrame([[101, 'baz', 10], [102, 'baz', 11],
                          [105, 'foo', 20], [105, 'baz', 21],
                          [106, 'bar', 15]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[101, 102, 106], [103, 104, 105]]
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='B',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        assert 'data' in result
        assert 'stats' in result

    def test_zscore_is_not_nan_if_data_misses_values(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6],
                          [102, 'foo', 10], [102, 'bar', 11],
                          [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value']),
            pd.DataFrame([[101, 'baz', 10], [102, 'baz', 11],
                          [105, 'foo', 20], [105, 'baz', 21],
                          [106, 'bar', 15]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[101, 102, 106], [103, 104, 105]]
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='B',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        data = result['data']
        data = pd.DataFrame(data)
        assert not np.isnan(np.min(data['zscore']))

    def test_results_are_sorted(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'A', 5], [102, 'A', 5],
                          [101, 'B', 2], [102, 'B', 2],
                          [101, 'C', 8], [102, 'C', 8],
                          [101, 'D', 10], [102, 'D', 10]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = []
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='mean',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        data = result['data']
        data = pd.DataFrame(data)
        feature_col = data['feature'].tolist()
        assert ['D', 'C', 'A', 'B', 'D', 'C', 'A', 'B'] == feature_col
        assert ['D', 'C', 'A', 'B'] == result['stats']['feature']

    def test_max_rows_works(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'A', 5], [102, 'A', 5],
                          [101, 'B', 2], [102, 'B', 2],
                          [101, 'C', 8], [102, 'C', 8],
                          [101, 'D', 10], [102, 'D', 10]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = []
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='mean',
                                params={},
                                id_filter=[],
                                max_rows=2,
                                subsets=subsets)
        data = result['data']
        data = pd.DataFrame(data)
        feature_col = data['feature'].tolist()
        assert ['D', 'C', 'D', 'C'] == feature_col
        assert result['stats']['feature'] == ['D', 'C']

    def test_sorts_correct_for_different_criteria(self):
        numerical_arrays = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', -12],
                          [102, 'foo', 10], [102, 'bar', -25],
                          [103, 'foo', 15], [103, 'bar', -20],
                          [104, 'foo', 20], [104, 'bar', -50]],
                         columns=['id', 'feature', 'value'])
        ]
        subsets = [[101, 102], [103, 104]]
        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='P.Value',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        stats = result['stats']['P.Value']
        assert all([stats[i] < stats[i + 1] for i in range(len(stats) - 1)])

        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='adj.P.Val',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        stats = result['stats']['adj.P.Val']
        assert all([stats[i] < stats[i + 1] for i in range(len(stats) - 1)])

        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='B',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        stats = result['stats']['B']
        assert all([stats[i] > stats[i + 1] for i in range(len(stats) - 1)])

        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='logFC',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        stats = result['stats']['logFC']
        assert all([abs(stats[i]) > abs(stats[i + 1])
                    for i in range(len(stats) - 1)])

        result = self.task.main(numerical_arrays=numerical_arrays,
                                numericals=[],
                                categoricals=[],
                                ranking_method='t',
                                params={},
                                id_filter=[],
                                max_rows=100,
                                subsets=subsets)
        stats = result['stats']['t']
        assert all([stats[i] > stats[i + 1] for i in range(len(stats) - 1)])

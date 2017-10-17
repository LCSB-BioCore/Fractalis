"""This module provides test for the pca task."""

import pandas as pd
import numpy as np

from fractalis.analytics.tasks.pca.main import PCATask


# noinspection PyMissingTypeHints
class TestPCATask:

    task = PCATask()

    def test_correct_output(self):
        features = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6],
                          [102, 'foo', 10], [102, 'bar', 11],
                          [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value']),
            pd.DataFrame([[101, 'baz', 5],
                          [102, 'baz', 10],
                          [104, 'baz', 20],
                          [105, 'baz', 100]],
                         columns=['id', 'feature', 'value'])
        ]
        categories = [
            pd.DataFrame([[101, '_', 'a'],
                          [102, '_', 'a'],
                          [104, '_', 'a']],
                         columns=['id', 'feature', 'value'])
        ]
        result = self.task.main(features=features,
                                categories=categories,
                                whiten=False,
                                id_filter=[],
                                subsets=[])
        data = result['data']
        assert 0 in data
        assert 1 in data
        assert 'category' in data
        assert 'subset' in data
        assert 'id' in data
        assert data['id'] == [101, 102, 103, 104, 105]
        assert data['subset'] == [0, 0, 0, 0, 0]
        np.testing.assert_equal(data['category'],
                                ['a', 'a', float('nan'), 'a', float('nan')])

    def test_id_filter_works(self):
        features = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 6],
                          [102, 'foo', 10], [102, 'bar', 11],
                          [103, 'foo', 15], [103, 'bar', 16],
                          [104, 'foo', 20], [104, 'bar', 21]],
                         columns=['id', 'feature', 'value'])
        ]
        result = self.task.main(features=features,
                                categories=[],
                                whiten=False,
                                id_filter=[101, 104],
                                subsets=[])
        data = result['data']
        assert all(np.unique(data['id']) == [101, 104])

    def test_correct_loadings(self):
        features = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 20],
                          [102, 'foo', 10], [102, 'bar', 15],
                          [103, 'foo', 15], [103, 'bar', 10],
                          [104, 'foo', 20], [104, 'bar', 5]],
                         columns=['id', 'feature', 'value'])
        ]
        result = self.task.main(features=features,
                                categories=[],
                                whiten=False,
                                id_filter=[],
                                subsets=[])
        loadings = result['loadings']
        assert loadings[0][0] == -loadings[0][1]
        assert loadings[1][0] == loadings[1][1]

    def test_correct_variance_ratios(self):
        features = [
            pd.DataFrame([[101, 'foo', 5], [101, 'bar', 5],
                          [102, 'foo', 10], [102, 'bar', 5],
                          [103, 'foo', 15], [103, 'bar', 5],
                          [104, 'foo', 20], [104, 'bar', 5]],
                         columns=['id', 'feature', 'value'])
        ]
        result = self.task.main(features=features,
                                categories=[],
                                whiten=False,
                                id_filter=[],
                                subsets=[])
        variance_ratios = result['variance_ratios']
        assert variance_ratios == [1, 0]

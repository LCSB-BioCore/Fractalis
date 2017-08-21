"""This module provides test for the pca task."""

import json

import pandas as pd

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
                                n_components=2,
                                whiten=False,
                                id_filter=[],
                                subsets=[])
        data = pd.read_json(result['data'])
        assert data.shape == (5, 5)
        assert '0' in list(data)
        assert '1' in list(data)
        assert 'category' in list(data)
        assert 'subset' in list(data)
        assert 'id' in list(data)
        assert data['id'].tolist() == [101, 102, 103, 104, 105]
        assert data['subset'].unique().tolist() == [0]
        assert data['category'].unique().tolist() == ['a', None]

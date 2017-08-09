"""This module provides tests for the cluster task
within the heatmap workflow."""

import json

import pytest

from fractalis.analytics.tasks.heatmap.cluster import ClusteringTask


# noinspection PyMissingOrEmptyDocstring,PyMethodMayBeStatic
class TestClustering:

    task = ClusteringTask()

    df = {
        'A': {
            'a': 50,
            'b': 2,
            'c': 45
        },
        'B': {
            'a': 250,
            'b': 5,
            'c': 300
        },
        'C': {
            'a': 55,
            'b': 4,
            'c': 60
        }
    }

    def test_hclust_raises_with_invalid_param_1(self):
        with pytest.raises(ValueError) as e:
            options = {
                'method': 'abc',
                'metric': 'euclidean',
                'n_row_clusters': 2,
                'n_col_clusters': 2
            }
            self.task.main(df=self.df, cluster_algo='hclust', options=options)
            assert 'Invalid method' in e

    def test_hclust_raises_with_invalid_param_2(self):
        with pytest.raises(ValueError) as e:
            options = {
                'method': 'single',
                'metric': 'abc',
                'n_row_clusters': 2,
                'n_col_clusters': 2
            }
            self.task.main(df=self.df, cluster_algo='hclust', options=options)
            assert 'Invalid metric' in e

    def test_hclust_raises_with_invalid_param_3(self):
        with pytest.raises(ValueError) as e:
            options = {
                'method': 'single',
                'metric': 'abc',
                'n_row_clusters': 2,
            }
            self.task.main(df=self.df, cluster_algo='hclust', options=options)
            assert 'mandatory parameters' in e

    def test_hclust_returns_valid_result(self):
        options = {
            'method': 'single',
            'metric': 'euclidean',
            'n_row_clusters': 2,
            'n_col_clusters': 2
        }
        result = self.task.main(df=self.df,
                                cluster_algo='hclust', options=options)
        assert 'row_names' in result
        assert 'col_names' in result
        assert 'row_cluster' in result
        assert 'col_cluster' in result
        assert ['a', 'c', 'b'] == result['row_names']
        assert ['A', 'C', 'B'] == result['col_names']
        assert [0, 0, 1] == result['row_cluster']
        assert [0, 0, 1] == result['col_cluster']

    def test_kmean_raises_with_invalid_param_1(self):
        with pytest.raises(ValueError) as e:
            options = {
                'n_row_centroids': 2,
                'n_col_centroids': 'abc'
            }
            self.task.main(df=self.df, cluster_algo='kmeans', options=options)
            assert 'invalid' in e

    def test_kmean_raises_with_invalid_param_2(self):
        with pytest.raises(ValueError) as e:
            options = {
                'n_row_centroids': 2,
            }
            self.task.main(df=self.df, cluster_algo='kmeans', options=options)
            assert 'mandatory parameters' in e

    def test_kmean_returns_valid_result(self):
        options = {
            'n_row_centroids': 2,
            'n_col_centroids': 2
        }
        result = self.task.main(df=self.df,
                                cluster_algo='kmeans', options=options)
        assert 'row_names' in result
        assert 'col_names' in result
        assert 'row_cluster' in result
        assert 'col_cluster' in result
        assert ['a', 'c', 'b'] == result['row_names']
        assert ['A', 'C', 'B'] == result['col_names']
        assert [0, 0, 1] == result['row_cluster']
        assert [0, 0, 1] == result['col_cluster']
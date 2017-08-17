"""This module provides tests for the cluster task
within the heatmap workflow."""

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

    def test_hclust_can_handle_identical_cluster_size(self):
        df = {
            'A': {
                'a': 5, 'b': 10
            },
            'B': {
                'a': 500, 'b': 550
            },
            'C': {
                'a': 5, 'b': 10
            },
            'D': {
                'a': 500, 'b': 550
            }
        }
        options = {
            'method': 'single',
            'metric': 'euclidean',
            'n_row_clusters': 2,
            'n_col_clusters': 2
        }
        result = self.task.main(df=df, cluster_algo='hclust', options=options)
        assert ['B', 'D', 'A', 'C'] == [x[0] for x in result['col_clusters']]
        assert [0, 0, 1, 1] == [x[1] for x in result['col_clusters']]

    def test_hclust_returns_valid_result(self):
        options = {
            'method': 'single',
            'metric': 'euclidean',
            'n_row_clusters': 2,
            'n_col_clusters': 2
        }
        result = self.task.main(df=self.df,
                                cluster_algo='hclust', options=options)
        assert 'row_clusters' in result
        assert 'col_clusters' in result
        assert ['a', 'c', 'b'] == [x[0] for x in result['row_clusters']]
        assert ['A', 'C', 'B'] == [x[0] for x in result['col_clusters']]
        assert [0, 0, 1] == [x[1] for x in result['col_clusters']]
        assert [0, 0, 1] == [x[1] for x in result['col_clusters']]

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

    def test_kmeans_can_handle_identical_cluster_size(self):
        df = {
            'A': {
                'a': 5, 'b': 10
            },
            'B': {
                'a': 500, 'b': 550
            },
            'C': {
                'a': 5, 'b': 10
            },
            'D': {
                'a': 500, 'b': 550
            }
        }
        options = {
            'n_row_centroids': 2,
            'n_col_centroids': 2
        }
        result = self.task.main(df=df, cluster_algo='kmeans', options=options)
        assert [0, 0, 1, 1] == [x[1] for x in result['col_clusters']]

    def test_kmean_returns_valid_result(self):
        options = {
            'n_row_centroids': 2,
            'n_col_centroids': 2
        }
        result = self.task.main(df=self.df,
                                cluster_algo='kmeans', options=options)
        assert 'row_clusters' in result
        assert 'col_clusters' in result
        assert ['a', 'c', 'b'] == [x[0] for x in result['row_clusters']]
        assert ['A', 'C', 'B'] == [x[0] for x in result['col_clusters']]
        assert [0, 0, 1] == [x[1] for x in result['col_clusters']]
        assert [0, 0, 1] == [x[1] for x in result['col_clusters']]

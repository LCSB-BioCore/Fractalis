import json

import pytest
import pandas as pd

from fractalis.analytics.tasks.histogram.main import HistogramTask


class TestHistogramTask:

    task = HistogramTask()

    def test_correct_output(self):
        df = pd.DataFrame([[100, 'foo', 1],
                           [101, 'foo', 2],
                           [102, 'foo', 3],
                           [103, 'foo', 4],
                           [104, 'foo', 5],
                           [105, 'foo', 6],
                           [106, 'foo', 7],
                           [107, 'foo', 8],
                           [108, 'foo', 9],
                           [109, 'foo', 10]],
                          columns=['id', 'feature', 'value'])
        cat_df = pd.DataFrame([[100, 'cat', 'A'],
                               [101, 'cat', 'B'],
                               [102, 'cat', 'A'],
                               [103, 'cat', 'B'],
                               [104, 'cat', 'A'],
                               [105, 'cat', 'B'],
                               [106, 'cat', 'A'],
                               [107, 'cat', 'B'],
                               [108, 'cat', 'A'],
                               [109, 'cat', 'B']],
                              columns=['id', 'feature', 'value'])
        result = self.task.main(id_filter=[],
                                bw_factor=0.5,
                                num_bins=10,
                                subsets=[],
                                data=df,
                                categories=[cat_df])
        assert all([key in result for key in
                    ['stats', 'subsets', 'categories', 'label']])
        assert 'A' in result['stats']
        assert 'B' in result['stats']
        assert 0 in result['stats']['A']
        assert all([stat in result['stats']['A'][0] for stat in
                    ['hist', 'bin_edges', 'mean', 'median', 'std', 'dist']])

    def test_can_handle_nas(self):
        df = pd.DataFrame([[100, 'foo', float('nan')],
                           [101, 'foo', 2],
                           [102, 'foo', float('nan')],
                           [103, 'foo', 4],
                           [104, 'foo', float('nan')],
                           [105, 'foo', 6],
                           [106, 'foo', float('nan')],
                           [107, 'foo', 8],
                           [108, 'foo', float('nan')],
                           [109, 'foo', 10]],
                          columns=['id', 'feature', 'value'])
        result = self.task.main(id_filter=[],
                                bw_factor=0.5,
                                num_bins=10,
                                subsets=[],
                                data=df,
                                categories=[])
        assert result['stats'][''][0]['median'] == 6
        assert result['stats'][''][0]['mean'] == 6

    def test_can_handle_negatives(self):
        df = pd.DataFrame([[100, 'foo', -2],
                           [101, 'foo', 2],
                           [102, 'foo', -4],
                           [103, 'foo', 4],
                           [104, 'foo', -6],
                           [105, 'foo', 6],
                           [106, 'foo', -8],
                           [107, 'foo', 8],
                           [108, 'foo', -10],
                           [109, 'foo', 10]],
                          columns=['id', 'feature', 'value'])
        result = self.task.main(id_filter=[],
                                bw_factor=0.5,
                                num_bins=10,
                                subsets=[],
                                data=df,
                                categories=[])
        assert result['stats'][''][0]['median'] == 0
        assert result['stats'][''][0]['mean'] == 0

    def test_skips_small_groups(self):
        df = pd.DataFrame([[100, 'foo', 1],
                           [101, 'foo', 2],
                           [102, 'foo', float('nan')],
                           [103, 'foo', 4],
                           [104, 'foo', float('nan')],
                           [105, 'foo', 6],
                           [106, 'foo', float('nan')],
                           [107, 'foo', 8],
                           [108, 'foo', float('nan')],
                           [109, 'foo', 10]],
                          columns=['id', 'feature', 'value'])
        cat_df = pd.DataFrame([[100, 'cat', 'A'],
                               [101, 'cat', 'B'],
                               [102, 'cat', 'A'],
                               [103, 'cat', 'B'],
                               [104, 'cat', 'A'],
                               [105, 'cat', 'B'],
                               [106, 'cat', 'A'],
                               [107, 'cat', 'B'],
                               [108, 'cat', 'A'],
                               [109, 'cat', 'B']],
                              columns=['id', 'feature', 'value'])
        result = self.task.main(id_filter=[],
                                bw_factor=0.5,
                                num_bins=10,
                                subsets=[],
                                data=df,
                                categories=[cat_df])
        assert 'A' not in result['stats']

    def test_skips_empty_groups(self):
        df = pd.DataFrame([[100, 'foo', float('nan')],
                           [101, 'foo', 2],
                           [102, 'foo', float('nan')],
                           [103, 'foo', 4],
                           [104, 'foo', float('nan')],
                           [105, 'foo', 6],
                           [106, 'foo', float('nan')],
                           [107, 'foo', 8],
                           [108, 'foo', float('nan')],
                           [109, 'foo', 10]],
                          columns=['id', 'feature', 'value'])
        cat_df = pd.DataFrame([[100, 'cat', 'A'],
                               [101, 'cat', 'B'],
                               [102, 'cat', 'A'],
                               [103, 'cat', 'B'],
                               [104, 'cat', 'A'],
                               [105, 'cat', 'B'],
                               [106, 'cat', 'A'],
                               [107, 'cat', 'B'],
                               [108, 'cat', 'A'],
                               [109, 'cat', 'B']],
                              columns=['id', 'feature', 'value'])
        result = self.task.main(id_filter=[],
                                bw_factor=0.5,
                                num_bins=10,
                                subsets=[],
                                data=df,
                                categories=[cat_df])
        assert 'A' not in result['stats']
        assert 'B' in result['stats']

    def test_throws_error_if_all_groups_empty(self):
        df = pd.DataFrame([[100, 'foo', float('nan')],
                           [101, 'foo', float('nan')],
                           [102, 'foo', float('nan')],
                           [103, 'foo', float('nan')],
                           [104, 'foo', float('nan')],
                           [105, 'foo', float('nan')],
                           [106, 'foo', float('nan')],
                           [107, 'foo', float('nan')],
                           [108, 'foo', float('nan')],
                           [109, 'foo', float('nan')]],
                          columns=['id', 'feature', 'value'])
        cat_df = pd.DataFrame([[100, 'cat', 'A'],
                               [101, 'cat', 'B'],
                               [102, 'cat', 'A'],
                               [103, 'cat', 'B'],
                               [104, 'cat', 'A'],
                               [105, 'cat', 'B'],
                               [106, 'cat', 'A'],
                               [107, 'cat', 'B'],
                               [108, 'cat', 'A'],
                               [109, 'cat', 'B']],
                              columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.task.main(id_filter=[],
                           bw_factor=0.5,
                           num_bins=10,
                           subsets=[],
                           data=df,
                           categories=[cat_df])
            assert 'selected numerical variable must be non-empty' in e

    def test_output_is_json_serializable(self):
        df = pd.DataFrame([[100, 'foo', 1],
                           [101, 'foo', 2],
                           [102, 'foo', 3],
                           [103, 'foo', 4],
                           [104, 'foo', 5],
                           [105, 'foo', 6],
                           [106, 'foo', 7],
                           [107, 'foo', 8],
                           [108, 'foo', 9],
                           [109, 'foo', 10]],
                          columns=['id', 'feature', 'value'])
        cat_df = pd.DataFrame([[100, 'cat', 'A'],
                               [101, 'cat', 'B'],
                               [102, 'cat', 'A'],
                               [103, 'cat', 'B'],
                               [104, 'cat', 'A'],
                               [105, 'cat', 'B'],
                               [106, 'cat', 'A'],
                               [107, 'cat', 'B'],
                               [108, 'cat', 'A'],
                               [109, 'cat', 'B']],
                              columns=['id', 'feature', 'value'])
        result = self.task.main(id_filter=[],
                                bw_factor=0.5,
                                num_bins=10,
                                subsets=[],
                                data=df,
                                categories=[cat_df])
        json.dumps(result)

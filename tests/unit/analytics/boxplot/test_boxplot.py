"""This module contains the tests for the Boxplot analysis code."""

import json

import numpy as np
import pandas as pd

from fractalis.analytics.tasks.boxplot.main import BoxplotTask


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestBoxplotAnalytics:

    task = BoxplotTask()

    def test_correct_output(self):
        df_1 = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                             [104, 'foo', 4]],
                            columns=['id', 'feature', 'value'])
        df_2 = pd.DataFrame([[101, 'bar', 1], [102, 'bar', 2], [103, 'bar', 3],
                             [104, 'bar', 4]],
                            columns=['id', 'feature', 'value'])
        results = self.task.main(features=[df_1, df_2],
                                 transformation='identity',
                                 categories=[],
                                 id_filter=[],
                                 subsets=[])
        json.dumps(results)  # check if result is json serializable
        assert 'data' in results
        assert 'statistics' in results
        assert 'anova' in results
        assert results['anova']['p_value'] == 1
        assert results['anova']['f_value'] == 0
        assert len(json.loads(results['data'])) == 8
        assert len(results['statistics']) == 2
        assert 'foo////s1' in results['statistics']
        assert 'bar////s1' in results['statistics']
        stats = results['statistics']['foo////s1']
        assert not np.isnan(stats['median'])
        assert not np.isnan(stats['l_qrt'])
        assert not np.isnan(stats['u_qrt'])
        assert not np.isnan(stats['l_wsk'])
        assert not np.isnan(stats['u_wsk'])

    def test_marks_outliers(self):
        df_1 = pd.DataFrame([[100, 'foo', -50],
                             [101, 'foo', 1],
                             [102, 'foo', 2],
                             [103, 'foo', 3],
                             [104, 'foo', 100]],
                            columns=['id', 'feature', 'value'])
        df_2 = pd.DataFrame([[201, 'bar', 1],
                             [202, 'bar', 2],
                             [203, 'bar', 3],
                             [204, 'bar', 100]],
                            columns=['id', 'feature', 'value'])
        results = self.task.main(features=[df_1, df_2], categories=[],
                                 transformation='identity',
                                 id_filter=[], subsets=[])
        df = pd.DataFrame.from_dict(json.loads(results['data']))
        assert np.all(df['outlier'] == [True, False, False, False, True,
                                        False, False, False, True])

    def test_can_handle_nan(self):
        df = pd.DataFrame([[100, 'foo', -50],
                           [101, 'foo', 1],
                           [102, 'foo', float('nan')],
                           [103, 'foo', 3],
                           [104, 'foo', 100]],
                          columns=['id', 'feature', 'value'])
        results = self.task.main(features=[df], categories=[],
                                 transformation='identity',
                                 id_filter=[], subsets=[])
        assert results['statistics']['foo////s1']['median'] == 2

    def test_can_handle_groups_with_only_nan(self):
        df = pd.DataFrame([[100, 'foo', -50],
                           [101, 'foo', 1],
                           [102, 'foo', float('nan')],
                           [103, 'foo', 3],
                           [104, 'foo', 100],
                           [105, 'foo', float('nan')]],
                          columns=['id', 'feature', 'value'])
        categories = pd.DataFrame([[100, 'gender', 'female'],
                                   [101, 'gender', 'female'],
                                   [102, 'gender', 'male'],
                                   [103, 'gender', 'female'],
                                   [104, 'gender', 'female'],
                                   [105, 'gender', 'male']],
                                  columns=['id', 'feature', 'value'])
        results = self.task.main(features=[df], categories=[categories],
                                 transformation='identity',
                                 id_filter=[], subsets=[])
        assert 'foo//female//s1' in results['statistics']
        assert 'foo//male//s1' not in results['statistics']

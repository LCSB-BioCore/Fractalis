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
                                 categories=[],
                                 id_filter=[],
                                 subsets=[])
        json.dumps(results)  # check if result is json serializable
        assert 'data' in results
        assert 'statistics' in results
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

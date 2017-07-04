"""This module contains the tests for the Boxplot analysis code."""

import json

import numpy as np
import pandas as pd

from fractalis.analytics.tasks.boxplot.main import BoxplotTask


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestBoxplotAnalytics:

    task = BoxplotTask()

    def test_functional_1(self):
        arr_1 = np.c_[range(10), np.random.randint(0, 100, size=(10, 1))]
        arr_2 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_3 = np.c_[range(5, 15), np.random.randint(0, 100, size=(10, 1))]
        arr_4 = np.c_[range(100, 102), np.random.randint(0, 100, size=(2, 1))]
        df_1 = pd.DataFrame(arr_1, columns=['id', 'A'])
        df_2 = pd.DataFrame(arr_2, columns=['id', 'B'])
        df_3 = pd.DataFrame(arr_3, columns=['id', 'C'])
        df_4 = pd.DataFrame(arr_4, columns=['id', 'D'])
        results = self.task.main(variables=[df_1, df_2, df_3, df_4],
                                 categories=[],
                                 id_filter=[],
                                 subsets=[])
        assert 'data' in results
        assert 'statistics' in results
        assert len(json.loads(results['data'])) == 22
        assert len(results['statistics']) == 4
        stats = results['statistics']['A////s1']
        assert not np.isnan(stats['median'])
        assert not np.isnan(stats['l_qrt'])
        assert not np.isnan(stats['u_qrt'])
        assert not np.isnan(stats['l_wsk'])
        assert not np.isnan(stats['u_wsk'])

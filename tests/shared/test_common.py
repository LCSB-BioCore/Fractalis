"""This module contains tests for the common module in the shared package."""

import pandas as pd
import numpy as np

from fractalis.analytics.tasks.shared import common


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestCommonTasks:

    def test_apply_subsets(self):
        df = pd.DataFrame([[1, 'a'], [2, 'a'], [3, 'a']], columns=['id', 'A'])
        result = common.apply_subsets(df=df, subsets=[[1, 2], [], [3, 4], [5], [2]])
        assert list(result['id']) == [1, 2, 3, 2]
        assert list(result['subset']) == [0, 0, 2, 4]

    def test_apply_empty_subsets(self):
        df = pd.DataFrame([[1, 'a'], [2, 'a'], [3, 'a']], columns=['id', 'A'])
        result = common.apply_subsets(df=df, subsets=[])
        assert list(result['subset']) == [0, 0, 0]

    def test_apply_categorys(self):
        df = pd.DataFrame([[1, 'a'], [2, 'a'], [3, 'a']], columns=['id', 'A'])
        category_1 = pd.DataFrame([[1, 'x']], columns=['id', 'x'])
        category_2 = pd.DataFrame([[1, 'y'], [2, 'y', 'z']],
                                    columns=['id', 'y', 'z'])
        result = common.apply_categories(df=df, categories=[category_1, category_2])
        assert list(result['category'])[0:2] == ['x&&y', 'y&&z']
        assert np.isnan(list(result['category'])[2])

    def test_apply_id_filter(self):
        df = pd.DataFrame([[1, 'a'], [2, 'a']], columns=['id', 'A'])
        result = common.apply_id_filter(df=df, id_filter=[2, 3])
        assert result.shape[0] == 1
        assert list(result['id']) == [2]

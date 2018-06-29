"""This module contains tests for the common module in the shared package."""

import pytest
import pandas as pd
import numpy as np

from fractalis.analytics.tasks.shared import utils


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestCommonTasks:

    def test_apply_subsets(self):
        df = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3]],
                          columns=['id', 'feature', 'value'])
        subsets = [[101, 102], [], [103, 102, 104]]
        result = utils.apply_subsets(df=df, subsets=subsets)
        assert result['subset'].tolist() == [0, 0, 2, 2]

    def test_apply_categorys(self):
        df = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3]],
                          columns=['id', 'feature', 'value'])
        c1 = pd.DataFrame([[101, 'c1', 'a'],
                           [102, 'c1', 'b'],
                           [105, 'c1', 'c']],
                          columns=['id', 'feature', 'value'])
        c2 = pd.DataFrame([[106, 'c2', 'd']],
                          columns=['id', 'feature', 'value'])
        c3 = pd.DataFrame([[102, 'c3', 'f']],
                          columns=['id', 'feature', 'value'])
        result = utils.apply_categories(df=df, categories=[c1, c2, c3])
        assert result['category'].tolist()[:2] == ['a', 'b AND f']
        assert np.isnan(result['category'].tolist()[2])

    def test_drop_unused_subset_ids(self):
        df = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3]],
                          columns=['id', 'feature', 'value'])
        subsets = [[], [101], [103, 104], [105]]
        subsets = utils.drop_unused_subset_ids(df=df, subsets=subsets)
        assert subsets == [[], [101], [103], []]

    def test_apply_transformation_raises_if_pos_inf(self):
        df = pd.DataFrame([[101, 'foo', 1000]],
                          columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            utils.apply_transformation(df=df, transformation='10^x')
            assert 'only be used on log scaled data' in e

    def test_apply_transformation_drops_zeros(self):
        df = pd.DataFrame([[101, 'foo', 1000], [102, 'foo', 0]],
                          columns=['id', 'feature', 'value'])
        assert df.shape[0] == 2
        df = utils.apply_transformation(df=df, transformation='log2(x)')
        assert df.shape[0] == 1

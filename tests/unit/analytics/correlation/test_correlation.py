"""Test suite for correlation analysis."""
import json

import pytest
import pandas as pd
import numpy as np

from fractalis.analytics.tasks.correlation.main import CorrelationTask


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestCorrelation:

    task = CorrelationTask()

    def test_correct_output(self):
        x = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                          [104, 'foo', 4]], columns=['id', 'feature', 'value'])
        y = pd.DataFrame([[101, 'bar', 1], [102, 'bar', 2], [103, 'bar', 3],
                          [104, 'bar', 4]], columns=['id', 'feature', 'value'])
        result = self.task.main(x=x,
                                y=y,
                                id_filter=[],
                                method='pearson',
                                subsets=[],
                                categories=[])
        assert result['coef'] == 1
        assert result['p_value'] == 0
        assert result['slope']
        assert result['intercept']
        assert result['method'] == 'pearson'
        assert result['data']
        assert result['x_label'] == 'foo'
        assert result['y_label'] == 'bar'
        data = json.loads(result['data'])
        assert 'id' in data[0]
        assert 'subset' in data[0]
        assert 'feature_x' in data[0]
        assert 'feature_y' in data[0]
        assert 'value_x' in data[0]
        assert 'value_y' in data[0]
        assert 'category' in data[0]

    def test_correct_shape(self):
        x = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                          [104, 'foo', 4]], columns=['id', 'feature', 'value'])
        y = pd.DataFrame([[101, 'bar', 1], [102, 'bar', 2], [103, 'bar', 3],
                          [104, 'bar', 4]], columns=['id', 'feature', 'value'])
        result = self.task.main(x=x,
                                y=y,
                                id_filter=[101, 102, 104],
                                method='pearson',
                                subsets=[[101, 102, 103], [102, 103, 104]],
                                categories=[])
        df = pd.DataFrame(json.loads(result['data']))
        assert df.shape == (4, 7)

    def test_empty_subset_equals_full_subset(self):
        x = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                          [104, 'foo', 4]], columns=['id', 'feature', 'value'])
        y = pd.DataFrame([[101, 'bar', 1], [102, 'bar', 2], [103, 'bar', 3],
                          [104, 'bar', 4]], columns=['id', 'feature', 'value'])
        result_1 = self.task.main(x=x,
                                  y=y,
                                  id_filter=[101, 102, 104],
                                  method='pearson',
                                  subsets=[[101, 102, 103, 104]],
                                  categories=[])
        result_2 = self.task.main(x=x,
                                  y=y,
                                  id_filter=[101, 104, 102],
                                  method='pearson',
                                  subsets=[],
                                  categories=[])
        assert result_1 == result_2

    def test_raises_for_unknown_method(self):
        x = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                          [104, 'foo', 4]], columns=['id', 'feature', 'value'])
        y = pd.DataFrame([[101, 'bar', 1], [102, 'bar', 2], [103, 'bar', 3],
                          [104, 'bar', 4]], columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='foo',
                           subsets=[],
                           categories=[])
            assert 'Unknown method' in e

    def test_x_and_y_intersect_must_be_non_empty(self):
        x = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                          [104, 'foo', 4]], columns=['id', 'feature', 'value'])
        y = pd.DataFrame([[201, 'bar', 1], [202, 'bar', 2], [203, 'bar', 3],
                          [204, 'bar', 4]], columns=['id', 'feature', 'value'])
        with pytest.raises(ValueError) as e:
            self.task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='pearson',
                           subsets=[],
                           categories=[])
            assert 'do not share any ids' in e

    def test_returns_nans_if_too_few_values(self):
        x = pd.DataFrame([[101, 'foo', 1], [102, 'foo', 2], [103, 'foo', 3],
                          [104, 'foo', 4]], columns=['id', 'feature', 'value'])
        y = pd.DataFrame([[101, 'bar', 1], [102, 'bar', 2], [103, 'bar', 3],
                          [104, 'bar', 4]], columns=['id', 'feature', 'value'])
        result = self.task.main(x=x,
                                y=y,
                                id_filter=[],
                                method='pearson',
                                subsets=[[101], [102, 104], [103], []],
                                categories=[])
        assert not np.isnan(result['coef'])

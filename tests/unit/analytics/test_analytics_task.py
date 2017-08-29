"""This module provides tests for the AnalyticsTask class."""

import pandas as pd

from fractalis.analytics.task import AnalyticTask


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class MockTask(AnalyticTask):
    @property
    def name(self) -> str:
        return ''

    def main(self, *args, **kwargs) -> dict:
        pass


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestAnalyticsTask:

    task = MockTask()

    def test_apply_filter(self):
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                          columns=['A', 'B', 'C'])
        filters = {'A': [1, 7], 'B': [2, 5, 8], 'C': [6, 9]}
        df = self.task.apply_filters(df=df, filters=filters)
        assert df.shape == (1, 3)
        assert df.iloc[0].tolist() == [7, 8, 9]

    def test_parse_value(self):
        arg1 = '${"id": 123, "filters": {"foo": [1,2]}}$'
        arg2 = '$123$'
        data_task_id, filters = self.task.parse_value(arg1)
        assert data_task_id == 123
        assert 'foo' in filters
        assert filters['foo'] == [1, 2]
        data_task_id, filters = self.task.parse_value(arg2)
        assert data_task_id == 123
        assert not filters

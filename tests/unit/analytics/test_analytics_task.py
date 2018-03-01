"""This module provides tests for the AnalyticsTask class."""

import pandas as pd

from uuid import uuid4
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
        filters1 = {'A': [1, 7], 'B': [2, 5, 8], 'C': [6, 9]}
        filters2 = {'A': []}
        df1 = self.task.apply_filters(df=df, filters=filters1)
        df2 = self.task.apply_filters(df=df, filters=filters2)
        assert df1.shape == (1, 3)
        assert df1.iloc[0].tolist() == [7, 8, 9]
        assert df2.shape == (3, 3)

    def test_parse_value(self):
        uuid = str(uuid4())
        arg1 = '${"id": 123, "filters": {"foo": [1,2]}}$'
        arg2 = '${}$'.format(uuid)
        arg3 = '${{"id": "{}", "filters": {{"foo": [1,2]}}}}$'.format(uuid)
        data_task_id, filters = self.task.parse_value(arg1)
        assert data_task_id is None
        assert 'foo' in filters
        assert filters['foo'] == [1, 2]
        data_task_id, filters = self.task.parse_value(arg2)
        assert data_task_id == uuid
        assert not filters
        data_task_id, filters = self.task.parse_value(arg3)
        assert data_task_id == uuid
        assert 'foo' in filters
        assert filters['foo'] == [1, 2]

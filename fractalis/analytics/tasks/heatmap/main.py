"""Module containing analysis code for heatmap analytics."""

from typing import List
from functools import reduce

import pandas as pd

from fractalis.analytics.task import AnalyticTask


class HeatmapTask(AnalyticTask):
    """Heatmap Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-heatmap'

    def main(self, numerical_arrays: List[pd.DataFrame],
             numericals: List[pd.DataFrame],
             categoricals: List[pd.DataFrame]) -> dict:
        df = reduce(lambda a, b: a.append(b), numerical_arrays)
        return {
            'data': df.to_json(orient='index')
        }
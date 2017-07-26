"""Module containing analysis code for heatmap analytics."""

from typing import List, TypeVar
from functools import reduce

import pandas as pd
from scipy.stats import zscore

from fractalis.analytics.task import AnalyticTask


T = TypeVar('T')


class HeatmapTask(AnalyticTask):
    """Heatmap Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-heatmap'

    def main(self, numerical_arrays: List[pd.DataFrame],
             numericals: List[pd.DataFrame],
             categoricals: List[pd.DataFrame],
             subsets: List[List[T]]) -> dict:
        df = reduce(lambda a, b: a.append(b), numerical_arrays)
        variables = df['variable']
        df = df.drop('variable', axis=1)
        zscores = df.apply(zscore, axis=1)

        #prepare output for front-end
        df = df.transpose()
        df.columns = variables
        df.index.name = 'id'
        df.reset_index(inplace=True)
        df = pd.melt(df, id_vars='id')

        zscores = zscores.transpose()
        zscores.columns = variables
        zscores.index.name = 'id'
        zscores.reset_index(inplace=True)
        zscores = pd.melt(zscores, id_vars='id')

        df = pd.merge(df, zscores, on=['id', 'variable'])
        df.columns = ['id', 'variable', 'value', 'zscore']

        return {
            'data': df.to_json(orient='index')
        }
"""Module containing analysis code for heatmap analytics."""

from typing import List, TypeVar
from functools import reduce
import logging

import pandas as pd
from scipy.stats import zscore

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.heatmap.stats import StatisticTask
from fractalis.analytics.tasks.shared import array_utils


T = TypeVar('T')
logger = logging.getLogger(__name__)


class HeatmapTask(AnalyticTask):
    """Heatmap Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-heatmap'
    stat_task = StatisticTask()

    def main(self, numerical_arrays: List[pd.DataFrame],
             numericals: List[pd.DataFrame],
             categoricals: List[pd.DataFrame],
             ranking_method: str,
             id_filter: List[T],
             subsets: List[List[T]]) -> dict:
        # prepare inputs args
        df = reduce(lambda a, b: a.append(b), numerical_arrays)
        if not subsets:
            ids = list(df)
            ids.remove('variable')
            subsets = [ids]
        df = array_utils.drop_ungrouped_samples(df=df, subsets=subsets)
        df = array_utils.apply_id_filter(df=df, id_filter=id_filter)
        subsets = array_utils.drop_unused_subset_ids(df=df, subsets=subsets)

        # make sure the input data are still valid after the pre-processing
        if df.shape[0] < 1 or df.shape[1] < 2:
            error = "Either the input data set is too small or " \
                    "the subset sample ids do not match the data."
            logger.error(error)
            raise ValueError(error)

        for subset in subsets:
            if not subset:
                error = "One or more of the specified subsets does not " \
                        "match any sample id for the given array data."
                logger.error(error)
                raise ValueError(error)

        # create z-score matrix used for visualising the heatmap
        variables = df['variable']
        zscores = df.drop('variable', axis=1)
        zscores = zscores.apply(zscore, axis=1)
        zscores.insert(0, 'variable', variables)

        # compute statistic for ranking
        stats = self.stat_task.main(df=df, subsets=subsets,
                                    ranking_method=ranking_method)

        # prepare output for front-end
        df = array_utils.melt_standard_format_df(df)
        zscores = array_utils.melt_standard_format_df(zscores)
        df = pd.merge(df, zscores, on=['id', 'variable'])
        df.columns = ['id', 'variable', 'value', 'zscore']

        return {
            'data': df.to_json(orient='index'),
            'stats': stats.to_json(orient='index')
        }

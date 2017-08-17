"""Module containing analysis code for heatmap analytics."""

from typing import List, TypeVar
from functools import reduce
import logging

import pandas as pd
from scipy.stats import zscore

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.heatmap.stats import StatisticTask
from fractalis.analytics.tasks.shared import utils


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
        # merge input data into single df
        df = reduce(lambda a, b: a.append(b), numerical_arrays)
        if not subsets:
            # empty subsets equals all samples in one subset
            subsets = [df['id'].unique().tolist()]
        else:
            # if subsets are defined we drop the rows that are not part of one
            flattened_subsets = [x for subset in subsets for x in subset]
            df = df[df['id'].isin(flattened_subsets)]
        # apply id filter
        if id_filter:
            df = df[df['id'].isin(id_filter)]
        # drop subset ids that are not in the df
        subsets = utils.drop_unused_subset_ids(df=df, subsets=subsets)
        # make sure the input data are still valid after the pre-processing
        if df.shape[0] < 1:
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

        # make matrix of input data
        _df = df.pivot(index='feature', columns='id', values='value')

        # create z-score matrix used for visualising the heatmap
        z_df = _df.apply(zscore, axis=1)

        # compute statistic for ranking
        stats = self.stat_task.main(df=_df, subsets=subsets,
                                    ranking_method=ranking_method)
        del _df

        # prepare output for front-end
        z_df['feature'] = z_df.index
        z_df = pd.melt(z_df, id_vars='feature')
        df = df.merge(z_df, on=['id', 'feature'])
        df.columns = ['id', 'feature', 'value', 'zscore']

        return {
            'data': df.to_json(orient='records'),
            'stats': stats.to_json(orient='records')
        }

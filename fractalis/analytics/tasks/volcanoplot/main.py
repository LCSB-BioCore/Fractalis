"""This module provides statistics for volcano plots."""

import logging
from typing import List, TypeVar
from functools import reduce

import pandas as pd

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared import utils, array_stats

T = TypeVar('T')
logger = logging.getLogger(__name__)


class VolcanoTask(AnalyticTask):
    """Volcanoplot Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-volcanoplot'

    def main(self, numerical_arrays: List[pd.DataFrame],
             id_filter: List[T],
             ranking_method: str,
             params: dict,
             subsets: List[List[T]]):
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

        # make matrix of input data
        df = df.pivot(index='feature', columns='id', values='value')
        features = list(df.index)

        # compute the stats (p / fC) for the selected ranking method
        stats = array_stats.get_stats(df=df,
                                      subsets=subsets,
                                      params=params,
                                      ranking_method=ranking_method)
        return {
            'features': features,
            'stats': stats.to_dict(orient='list')
        }

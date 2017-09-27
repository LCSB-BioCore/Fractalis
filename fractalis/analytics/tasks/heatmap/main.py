"""Module containing analysis code for heatmap analytics."""

from typing import List, TypeVar
from functools import reduce
import logging

import pandas as pd

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
             max_rows: int,
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

        # make matrix of input data
        df = df.pivot(index='feature', columns='id', values='value')

        # create z-score matrix used for visualising the heatmap
        z_df = [(df.iloc[i] - df.iloc[i].mean()) / df.iloc[i].std(ddof=0)
                for i in range(df.shape[0])]
        z_df = pd.DataFrame(z_df, columns=df.columns, index=df.index)

        # compute statistic for ranking
        stats = self.stat_task.main(df=df, subsets=subsets,
                                    ranking_method=ranking_method)

        # sort by ranking_value
        self.sort(df, stats[ranking_method], ranking_method)
        self.sort(z_df, stats[ranking_method], ranking_method)
        self.sort(stats, stats[ranking_method], ranking_method)

        # discard rows according to max_rows
        df = df[:max_rows]
        z_df = z_df[:max_rows]
        stats = stats[:max_rows]

        # prepare output for front-end
        df['feature'] = df.index
        z_df['feature'] = z_df.index
        df = pd.melt(df, id_vars='feature', var_name='id')
        z_df = pd.melt(z_df, id_vars='feature', var_name='id')
        df = df.merge(z_df, on=['id', 'feature'])
        df.rename(columns={'value_x': 'value', 'value_y': 'zscore'},
                  inplace=True)
        df = utils.apply_subsets(df, subsets)

        return {
            'data': df.to_dict(orient='list'),
            'stats': stats.to_dict(orient='list')
        }

    @staticmethod
    def sort(df, order, method):
        order = order.tolist()
        if method == 'P.Value' or method == 'adj.P.Val':
            order = [1 - x for x in order]
        elif method == 'logFC' or method == 't':
            order = [abs(x) for x in order]
        df['sort_value'] = order
        df.sort_values('sort_value', ascending=False, inplace=True)
        df.drop('sort_value', axis=1, inplace=True)

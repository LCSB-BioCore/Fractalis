"""This module contains several statistics necessary for creating a
histogram."""

import logging
from typing import List

import pandas as pd
import numpy as np

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared import utils


logger = logging.getLogger(__name__)


class HistogramTask(AnalyticTask):
    """Histogram Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-histogram'

    def main(self,
             id_filter: List[str],
             subsets: List[List[str]],
             data: pd.DataFrame,
             categories: List[pd.DataFrame]) -> dict:
        """Compute several basic statistics such as bin size and variance.
        :param id_filter: If specified use only given ids during the analysis.
        :param subsets: List of lists of subset ids.
        :param data: Numerical values to create histogram of.
        :param categories: The groups to split the values into.
        """
        df = data
        del data
        df.dropna(inplace=True)
        if df.shape[0] == 0:
            error = 'The selected numerical variable must be non-empty.'
            logger.exception(error)
            raise ValueError(error)
        df = utils.apply_id_filter(df=df, id_filter=id_filter)
        df = utils.apply_subsets(df=df, subsets=subsets)
        df = utils.apply_categories(df=df, categories=categories)
        stats = {}
        categories = df['category'].unique().tolist()
        subsets = df['subset'].unique().tolist()
        for category in categories:
            for subset in subsets:
                sub_df = df[(df['category'] == category) &
                            (df['subset'] == subset)]
                values = sub_df['value']
                hist, bin_edges = np.histogram(values)
                hist = list(hist)
                bin_edges = list(bin_edges)
                mean = np.mean(values)
                median = np.median(values)
                variance = np.var(values)
                if not stats.get(category):
                    stats[category] = {}
                stats[category][subset] = {
                    'hist': hist,
                    'bin_edges': bin_edges,
                    'mean': mean,
                    'median': median,
                    'variance': variance
                }
        return {
            'stats': stats
        }

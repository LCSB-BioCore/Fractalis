"""This module contains several statistics necessary for creating a
histogram."""

import logging
from functools import partial
from typing import List

import pandas as pd
import numpy as np
import scipy.stats

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared import utils


logger = logging.getLogger(__name__)


class HistogramTask(AnalyticTask):
    """Histogram Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-histogram'

    def main(self,
             bw_factor: float,
             num_bins: int,
             id_filter: List[str],
             subsets: List[List[str]],
             data: pd.DataFrame,
             categories: List[pd.DataFrame]) -> dict:
        """Compute several basic statistics such as bin size and kde.
        :param bw_factor: KDE resolution.
        :param num_bins: Number of bins to use for histogram.
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
                if values.shape[0] < 2:
                    continue
                hist, bin_edges = np.histogram(values, bins=num_bins)
                hist = hist.tolist()
                bin_edges = bin_edges.tolist()
                mean = np.mean(values)
                median = np.median(values)
                std = np.std(values)

                def bw(obj, fac):
                    return np.power(obj.n, -1.0 / (obj.d + 4)) * fac

                kde = scipy.stats.gaussian_kde(
                    values, bw_method=partial(bw, fac=bw_factor))
                xs = np.linspace(
                    start=np.min(values), stop=np.max(values), num=200)
                dist = kde(xs).tolist()
                if not stats.get(category):
                    stats[category] = {}
                stats[category][subset] = {
                    'hist': hist,
                    'bin_edges': bin_edges,
                    'mean': mean,
                    'median': median,
                    'std': std,
                    'dist': dist
                }
        return {
            'data': df.to_json(orient='records'),
            'stats': stats,
            'subsets': subsets,
            'categories': categories,
            'label': df['feature'].tolist()[0]
        }

"""Module containing the Celery Task for the Correlation Analysis."""

import logging
from typing import List, TypeVar

import pandas as pd
import numpy as np
from scipy import stats

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared.utils import \
    apply_subsets, apply_categories


logger = logging.getLogger(__name__)
T = TypeVar('T')


class CorrelationTask(AnalyticTask):
    """Correlation Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-correlation'

    def main(self,
             x: pd.DataFrame,
             y: pd.DataFrame,
             id_filter: List[T],
             method: str,
             subsets: List[List[T]],
             categories: List[pd.DataFrame]) -> dict:
        """Compute correlation statistics for the given parameters.
        :param x: DataFrame containing x axis values.
        :param y: DataFrame containing y axis values.
        :param id_filter: If specified use only given ids during the analysis.
        :param method: pearson, spearman or kendall.
        :param subsets: List of lists of subset ids.
        :param categories: List of DataFrames that categorise the data points.
        :return: corr. coef., p-value and other useful values.
        """
        if len(x['feature'].unique().tolist()) != 1 \
                or len(y['feature'].unique().tolist()) != 1:
            error = "Input is invalid. Please make sure that the two " \
                    "variables to compare have exactly one dimension, each."
            logger.error(error)
            raise ValueError(error)
        if method not in ['pearson', 'spearman', 'kendall']:
            raise ValueError("Unknown method '{}'".format(method))

        df = self.merge_x_y(x, y)
        (x_label, y_label) = (df['feature_x'][0], df['feature_y'][0])
        if id_filter:
            df = df[df['id'].isin(id_filter)]
        df = apply_subsets(df=df, subsets=subsets)
        df = apply_categories(df=df, categories=categories)
        global_stats = self.compute_stats(df, method)
        subset_dfs = [df[df['subset'] == i] for i in range(len(subsets) or 1)]
        subset_stats = [self.compute_stats(subset_df, method)
                        for subset_df in subset_dfs]

        output = global_stats
        output['subsets'] = subset_stats
        output['method'] = method
        output['data'] = df.to_json(orient='records')
        output['x_label'] = x_label
        output['y_label'] = y_label

        return output

    @staticmethod
    def merge_x_y(x: pd.DataFrame, y: pd.DataFrame) -> pd.DataFrame:
        """Merge the x and y DataFrame and drop all rows containing NA.
        :param x: The x-axis values.
        :param y: The y-axis values.
        :return: The merged data frame.
        """
        df = x.merge(y, on=['id'], how='inner')
        df = df.dropna()
        if df.shape[0] == 0:
            raise ValueError("X and Y do not share any ids.")
        return df

    @staticmethod
    def compute_stats(df: pd.DataFrame, method: str) -> dict:
        """Compute correlation statistics for the given data and the given
        correlation method.
        :param df: The DataFrame containing our data.
        :param method: The method to use.
        :param x_label: The name of the x-axis.
        :param y_label: The name of the y-axis.
        :return: Several relevant statistics
        """
        df = df.drop_duplicates('id')
        df = df.dropna()
        if df.shape[0] < 2:
            return {
                'coef': float('nan'),
                'p_value': float('nan'),
                'slope': float('nan'),
                'intercept': float('nan')
            }
        if method == 'pearson':
            corr_coef, p_value = stats.pearsonr(df['value_x'], df['value_y'])
        elif method == 'spearman':
            corr_coef, p_value = stats.spearmanr(df['value_x'], df['value_y'])
        elif method == 'kendall':
            corr_coef, p_value = stats.kendalltau(df['value_x'], df['value_y'])
        else:
            raise ValueError("Unknown correlation method.")
        slope, intercept, *_ = np.polyfit(df['value_x'], df['value_y'], deg=1)
        return {
            'coef': corr_coef,
            'p_value': p_value,
            'slope': slope,
            'intercept': intercept,
        }

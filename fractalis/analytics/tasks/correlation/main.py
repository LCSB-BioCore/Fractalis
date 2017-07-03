"""Module containing the Celery Task for the Correlation Analysis."""

from typing import List, TypeVar, Tuple

import pandas as pd
import numpy as np
from scipy import stats

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared.common import \
    apply_subsets, apply_categories, apply_id_filter


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
             annotations: List[pd.DataFrame]) -> dict:
        """Compute correlation statistics for the given parameters.
        :param x: DataFrame containing x axis values.
        :param y: DataFrame containing y axis values.
        :param id_filter: If specified use only given ids during the analysis.
        :param method: pearson, spearman or kendall.
        :param subsets: List of lists of subset ids.
        :param annotations: List of DataFrames that annotate the data points.
        :return: corr. coef., p-value and other useful values.
        """
        if x.shape[0] == 0 or y.shape[0] == 0:
            raise ValueError("X or Y contain no data.")
        if x.shape[1] < 2 or y.shape[1] < 2:
            raise ValueError("X or Y are malformed.")
        if method not in ['pearson', 'spearman', 'kendall']:
            raise ValueError("Unknown method '{}'".format(method))

        df = self.merge_x_y(x, y)
        (x_label, y_label) = self.get_axis_labels(df)
        df = apply_id_filter(df=df, id_filter=id_filter)
        df = apply_subsets(df=df, subsets=subsets)
        df = apply_categories(df=df, categories=annotations)
        global_stats = self.compute_stats(df, method, x_label, y_label)
        subset_dfs = [df[df['subset'] == i] for i in range(len(subsets) or 1)]
        subset_stats = [self.compute_stats(subset_df, method, x_label, y_label)
                        for subset_df in subset_dfs]

        output = global_stats
        output['subsets'] = subset_stats
        output['method'] = method
        output['data'] = df.to_json(orient='index')
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
        df = x.merge(y, on='id', how='inner')
        df = df.dropna()
        if df.shape[0] == 0:
            raise ValueError("X and Y do not share any ids.")
        return df

    @staticmethod
    def get_axis_labels(df: pd.DataFrame) -> Tuple[str, str]:
        """Extract axis labels from the given DataFrame.
        :param df: DataFrame that has contains ids axis labels and possibly more
        :return: A tuple containing both labels.
        """
        colnames = [name for name in list(df) if name != 'id']
        assert len(colnames) == 2
        x_label = colnames[0]
        y_label = colnames[1]
        return (x_label, y_label)

    @staticmethod
    def compute_stats(df: pd.DataFrame, method: str,
                      x_label: str, y_label: str) -> dict:
        """Compute correlation statistics for the given data and the given
        correlation method.
        :param df: The DataFrame containing our data. 
        :param method: The method to use.
        :param x_label: The name of the x-axis.
        :param y_label: The name of the y-axis.
        :return: Coefficient, p-value, regression slope and regression intercept
        """
        df = df.drop_duplicates('id')
        df = df.dropna()
        df = df[[x_label, y_label]]
        if df.shape[0] < 2:
            return {
                'coef': float('nan'),
                'p_value': float('nan'),
                'slope': float('nan'),
                'intercept': float('nan')
            }
        x_list = df[x_label].values.tolist()
        y_list = df[y_label].values.tolist()
        if method == 'pearson':
            corr_coef, p_value = stats.pearsonr(x_list, y_list)
        elif method == 'spearman':
            corr_coef, p_value = stats.spearmanr(x_list, y_list)
        elif method == 'kendall':
            corr_coef, p_value = stats.kendalltau(x_list, y_list)
        else:
            raise ValueError("Unknown correlation method.")
        slope, intercept, *_ = np.polyfit(x_list, y_list, deg=1)
        return {
            'coef': corr_coef,
            'p_value': p_value,
            'slope': slope,
            'intercept': intercept,
        }

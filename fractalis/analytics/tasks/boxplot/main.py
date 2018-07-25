"""Module containing the Celery task for Boxplot statistics."""

from typing import List, TypeVar
from functools import reduce

import pandas as pd
import numpy as np
import scipy.stats

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared import utils


T = TypeVar('T')


class BoxplotTask(AnalyticTask):
    """Boxplot Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-boxplot'

    def main(self,
             features: List[pd.DataFrame],
             categories: List[pd.DataFrame],
             id_filter: List[T],
             transformation: str,
             subsets: List[List[T]]) -> dict:
        """ Compute boxplot statistics for the given parameters.
        :param features: List of numerical features
        :param categories: List of categorical features used to group numerical
        features.
        :param id_filter: List of ids that will be considered for analysis. If
        empty all ids will be used.
        :param transformation: Transformation that will be applied to the data.
        :param subsets: List of subsets used as another way to group the
        numerical features.
        """
        if not len(features):
            raise ValueError("Must at least specify one "
                             "non empty numerical feature.")
        # merge dfs into single one
        df = reduce(lambda l, r: l.append(r), features)
        df = utils.apply_transformation(df=df, transformation=transformation)
        df.dropna(inplace=True)
        df = utils.apply_id_filter(df=df, id_filter=id_filter)
        df = utils.apply_subsets(df=df, subsets=subsets)
        df = utils.apply_categories(df=df, categories=categories)
        df['outlier'] = None
        results = {
            'statistics': {},
            'features': df['feature'].unique().tolist(),
            'categories': df['category'].unique().tolist(),
            'subsets': df['subset'].unique().tolist()
        }
        group_values = []
        for feature in results['features']:
            for subset in results['subsets']:
                for category in results['categories']:
                    values = df[(df['subset'] == subset) &
                                (df['category'] == category) &
                                (df['feature'] == feature)]['value'].tolist()
                    if len(values) < 2:
                        continue
                    # FIXME: v This is ugly. Look at kaplan_meier_survival.py
                    label = '{}//{}//s{}'.format(feature, category, subset + 1)
                    group_values.append(values)
                    stats = self.boxplot_statistics(values)
                    u_outliers = np.array(values) > stats['u_wsk']
                    l_outliers = np.array(values) < stats['l_wsk']
                    outliers = np.bitwise_or(u_outliers, l_outliers)
                    df.loc[(df['subset'] == subset) &
                           (df['category'] == category) &
                           (df['feature'] == feature), 'outlier'] = outliers
                    kde = scipy.stats.gaussian_kde(values)
                    xs = np.linspace(start=stats['l_wsk'],
                                     stop=stats['u_wsk'], num=100)
                    stats['kde'] = kde(xs).tolist()
                    results['statistics'][label] = stats
        results['data'] = df.to_json(orient='records')
        f_value, p_value = scipy.stats.f_oneway(*group_values)
        results['anova'] = {
            'p_value': p_value,
            'f_value': f_value
        }
        return results

    @staticmethod
    def boxplot_statistics(values: List[float]) -> dict:
        """Compute boxplot statistics for the given values.
        :param values: A one dimensional list of numbers.
        :return: A dictionary containing all important boxplot statistics.
        """
        l_qrt = np.percentile(values, 25)
        median = np.percentile(values, 50)
        u_qrt = np.percentile(values, 75)
        iqr = u_qrt - l_qrt
        values = sorted(values)
        # lower whisker as defined by John W. Tukey
        l_wsk = next(value for value in values if value >= l_qrt - 1.5 * iqr)
        values = values[::-1]
        # upper whisker as defined by John W. Tukey
        u_wsk = next(value for value in values if value <= u_qrt + 1.5 * iqr)
        return {
            'l_qrt': l_qrt,
            'median': median,
            'u_qrt': u_qrt,
            'l_wsk': l_wsk,
            'u_wsk': u_wsk
        }

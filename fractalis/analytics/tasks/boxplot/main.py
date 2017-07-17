"""Module containing the Celery task for Boxplot statistics."""

from typing import List, TypeVar
from functools import reduce

import pandas as pd
import numpy as np
import scipy.stats

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared.common import \
    apply_subsets, apply_categories, apply_id_filter


T = TypeVar('T')


class BoxplotTask(AnalyticTask):
    """Boxplot Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-boxplot'

    def main(self,
             variables: List[pd.DataFrame],
             categories: List[pd.DataFrame],
             id_filter: List[T],
             subsets: List[List[T]]) -> dict:
        """ Compute boxplot statistics for the given parameters.
        :param variables: List of numerical variables
        :param categories: List of categorical variables used to group numerical
        variables.
        :param id_filter: List of ids that will be considered for analysis. If
        empty all ids will be used.
        :param subsets: List of subsets used as another way to group the
        numerical variables.
        """
        if not len(variables):
            raise ValueError("Must at least specify one "
                             "non empty numerical variable.")
        df = reduce(lambda l, r: l.merge(r, on='id', how='outer'), variables)
        df = apply_id_filter(df=df, id_filter=id_filter)
        variable_names = df.columns.tolist()
        variable_names.remove('id')
        df = apply_subsets(df=df, subsets=subsets)
        df = apply_categories(df=df, categories=categories)
        results = {
            'data': df.to_json(orient='index'),
            'statistics': {},
            'variables': variable_names,
            'categories': list(set(df['category'].tolist())),
            'subsets': list(set(df['subset'].tolist()))
        }
        for variable in variable_names:
            for subset in results['subsets']:
                for category in results['categories']:
                    values = df[(df['subset'] == subset) & (df['category'] == category)][variable].tolist()
                    values = [value for value in values if not np.isnan(value)]
                    if len(values) < 2:
                        continue
                    stats = self.boxplot_statistics(values)
                    kde = scipy.stats.gaussian_kde(values)
                    xs = np.linspace(start=stats['l_wsk'],
                                     stop=stats['u_wsk'], num=100)
                    stats['kde'] = kde(xs).tolist()
                    results['statistics']['{}//{}//s{}'.format(variable, category, subset + 1)] = stats
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
        values.sort()
        # lower whisker as defined by John W. Tukey
        l_wsk = next(value for value in values if value >= l_qrt - 1.5 * iqr)
        values.sort(reverse=True)
        # upper whisker as defined by John W. Tukey
        u_wsk = next(value for value in values if value <= u_qrt + 1.5 * iqr)
        return {
            'l_qrt': l_qrt,
            'median': median,
            'u_qrt': u_qrt,
            'l_wsk': l_wsk,
            'u_wsk': u_wsk
        }
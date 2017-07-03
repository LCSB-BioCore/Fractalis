"""Module containing the Celery task for Boxplot statistics."""

from typing import List, TypeVar
from functools import reduce

import pandas as pd
import numpy as np

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared.common import \
    apply_subsets, apply_categories


T = TypeVar('T')


class BoxplotTask(AnalyticTask):
    """Boxplot Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-boxplot'

    def main(self,
             variables: List[pd.DataFrame],
             categories: List[pd.DataFrame],
             subsets: List[List[T]]) -> dict:
        """ Compute boxplot statistics for the given parameters.
        :param variables: List of numerical variables
        :param categories: List of categorical variables used to group numerical
        variables.
        :param subsets: List of subsets used as another way to group the
        numerical variables.
        """
        df = reduce(lambda l, r: l.merge(r, on='id', how='outer'), variables)
        df = apply_subsets(df=df, subsets=subsets)
        variable_names = df.columns.tolist()
        variable_names.remove('id')
        df = apply_categories(df=df, categories=categories)
        results = {
            'data': df.to_json(orient='index'),
            'statistics': {}
        }
        for variable in variable_names:
            for subset in list(set(df['subset'].tolist())):
                for category in list(set(df['category'].tolist())):
                    values = df[(df['subset'] == subset) & (df['category'] == category)][variable]
                    stats = self.boxplot_statistics(values)
                    results['statistics'][variable][category][subset] = stats
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
        return {
            'l_qrt': l_qrt,
            'median': median,
            'u_qrt': u_qrt,
            'l_wsk': l_qrt - 1.5 * iqr,
            'u_wsk': u_qrt + 1.5 * iqr
        }

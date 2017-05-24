from typing import List

import pandas as pd
import numpy as np
from scipy import stats

from fractalis.analytics.task import AnalyticTask


class CorrelationTask(AnalyticTask):

    name = 'compute-correlation'

    def main(self, x: pd.DataFrame, y: pd.DataFrame, id_filter: List[str],
             method: str, subsets: List[List[str]]) -> dict:
        if x.shape[0] == 0 or y.shape[0] == 0:
            raise ValueError("X or Y contain no data.")
        if x.shape[1] < 2 or y.shape[1] < 2:
            raise ValueError("X or Y are malformed.")
        if method not in ['pearson', 'spearman', 'kendall']:
            raise ValueError("Unknown method '{}'".format(method))
        if len(subsets) == 0:
            raise ValueError("No subsets specified.")

        df = pd.merge(x, y, on='id')
        df = df.dropna()
        if df.shape[0] == 0:
            raise ValueError("X and Y do not share any ids.")

        if id_filter:
            df = df[df['id'].isin(id_filter)]
        if df.shape[0] == 0:
            raise ValueError("The current selection does not match any data.")

        output = {
            'subsets': {}
        }

        _df = pd.DataFrame()
        for i, subset in enumerate(subsets):
            df_subset = df[df['id'].isin(subset)]
            subset_col = pd.Series([i] * df_subset.shape[0])
            df_subset = df_subset.assign(subset=subset_col)
            output['subsets'][i] = self.compute_stats(df_subset)
            _df = _df.append(df_subset)
        df = _df
        del _df
        if df.shape[0] == 0:
            raise ValueError("No data match given subsets. Keep in mind that X "
                             "and Y are intersected before the subsets are "
                             "applied.")

        global_stats = self.compute_stats(df.drop_duplicates('id'))

        output.update(global_stats)
        output['method'] = method
        output['data'] = df.to_json()
        output['x_label'] = list(df)[0]
        output['y_label'] = list(df)[1]
        return output

    @staticmethod
    def compute_stats(df: pd.DataFrame) -> dict:
        if df.shape[0] < 2:
            return {
                'coef': float('nan'),
                'p_value': float('nan'),
                'slope': float('nan'),
                'intercept': float('nan')
            }
        df = df.drop('id', 1)
        x_list = df.ix[:, 0].values.tolist()
        y_list = df.ix[:, 1].values.tolist()
        corr_coef, p_value = stats.pearsonr(x_list, y_list)
        slope, intercept, *_ = np.polyfit(x_list, y_list, deg=1)
        return {
            'coef': corr_coef,
            'p_value': p_value,
            'slope': slope,
            'intercept': intercept,
        }

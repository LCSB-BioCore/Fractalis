from typing import List
from functools import reduce

import pandas as pd
import numpy as np
from scipy import stats

from fractalis.analytics.task import AnalyticTask


class CorrelationTask(AnalyticTask):

    name = 'compute-correlation'

    def main(self,
             x: pd.DataFrame,
             y: pd.DataFrame,
             id_filter: List[str],
             method: str,
             subsets: List[List[str]],
             annotations: List[pd.DataFrame]) -> dict:

        if x.shape[0] == 0 or y.shape[0] == 0:
            raise ValueError("X or Y contain no data.")
        if x.shape[1] < 2 or y.shape[1] < 2:
            raise ValueError("X or Y are malformed.")
        if method not in ['pearson', 'spearman', 'kendall']:
            raise ValueError("Unknown method '{}'".format(method))

        df = self.merge_x_y(x, y)
        x_label = list(df)[1]
        y_label = list(df)[2]
        df = self.apply_id_filter(df, id_filter)
        df = self.apply_subsets(df, subsets)
        df = self.apply_annotations(annotations, df)
        global_stats = self.compute_stats(df, x_label, y_label)
        subset_dfs = [df[df['subset'] == i] for i in range(len(subsets) or 1)]
        subset_stats = [self.compute_stats(subset_df, x_label, y_label)
                        for subset_df in subset_dfs]

        output = global_stats
        output['subsets'] = subset_stats
        output['method'] = method
        output['data'] = df.to_json()
        output['x_label'] = x_label
        output['y_label'] = y_label

        return output

    @staticmethod
    def merge_x_y(x: pd.DataFrame, y: pd.DataFrame) -> pd.DataFrame:
        df = x.merge(y, on='id', how='inner')
        df = df.dropna()
        if df.shape[0] == 0:
            raise ValueError("X and Y do not share any ids.")
        return df

    @staticmethod
    def apply_id_filter(df: pd.DataFrame, id_filter: list) -> pd.DataFrame:
        if id_filter:
            df = df[df['id'].isin(id_filter)]
        if df.shape[0] == 0:
            raise ValueError("The current selection does not match any data.")
        return df

    @staticmethod
    def apply_subsets(df: pd.DataFrame,
                      subsets: List[List[str]]) -> pd.DataFrame:
        if not subsets:
            subsets = [df['id']]
        _df = pd.DataFrame()
        for i, subset in enumerate(subsets):
            df_subset = df[df['id'].isin(subset)]
            subset_col = pd.Series([i] * df_subset.shape[0])
            df_subset = df_subset.assign(subset=subset_col)
            _df = _df.append(df_subset)
        if _df.shape[0] == 0:
            raise ValueError("No data match given subsets. Keep in mind that X "
                             "and Y are intersected before the subsets are "
                             "applied.")
        return _df
    @staticmethod
    def apply_annotations(annotations: List[pd.DataFrame],
                          df: pd.DataFrame) -> pd.DataFrame:
        if annotations:
            annotation_data = reduce(
                lambda l, r: l.merge(r, on='id', how='outer'), annotations)\
                .drop('id', axis=1)\
                .apply(
                lambda row: ' & '.join(list(map(str, row.tolist()))), axis=1)
            annotation_df = pd.DataFrame(columns=['annotation'],
                                         data=annotation_data)
            df = df.merge(annotation_df, on='id', how='left')
        return df

    @staticmethod
    def compute_stats(df: pd.DataFrame, x_label: str, y_label: str) -> dict:
        df = df.drop_duplicates('id')
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
        corr_coef, p_value = stats.pearsonr(x_list, y_list)
        slope, intercept, *_ = np.polyfit(x_list, y_list, deg=1)
        return {
            'coef': corr_coef,
            'p_value': p_value,
            'slope': slope,
            'intercept': intercept,
        }

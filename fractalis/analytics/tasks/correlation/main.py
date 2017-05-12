import pandas as pd
import numpy as np
from scipy import stats

from fractalis.analytics.task import AnalyticTask


class CorrelationTask(AnalyticTask):

    name = 'compute-correlation'

    def main(self, x, y, ids, method='pearson'):
        df = pd.merge(x, y, on='id')
        df = df.dropna()
        if ids:
            df = df[df['id'].isin(ids)]
        df_noid = df.drop('id', 1)
        x_list = df_noid.ix[:, 0].values.tolist()
        y_list = df_noid.ix[:, 1].values.tolist()
        corr_coef, p_value = stats.pearsonr(x_list, y_list)
        slope, intercept, *_ = np.polyfit(x_list, y_list, deg=1)
        return {
            'coef': corr_coef,
            'p_value': p_value,
            'slope': slope,
            'intercept': intercept,
            'method': method,
            'data': df.to_json(),
            'x_label': list(df_noid)[0],
            'y_label': list(df_noid)[1]
        }

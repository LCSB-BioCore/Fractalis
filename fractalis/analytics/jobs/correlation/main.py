import pandas as pd

from fractalis.analytics.job import AnalyticsJob

class CorrelationJob(AnalyticsJob):

    name = 'compute-correlation'

    def main(self, x, y):
        pd.merge(x, y, on='id')

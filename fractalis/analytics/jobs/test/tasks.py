import json
import time

from fractalis.analytics.job import AnalyticsJob


class AddJob(AnalyticsJob):

    name = 'add_test_job'

    def main(self, a, b):
        result = {}
        result['sum'] = a + b
        return json.dumps(result)


class DoNothingJob(AnalyticsJob):

    name = 'nothing_test_job'

    def main(self, seconds):
        result = {}
        time.sleep(seconds)
        result['foo'] = 'bar'
        return json.dumps(result)


class DivJob(AnalyticsJob):

    name = 'div_test_job'

    def main(self, a, b):
        result = {}
        result['div'] = a / b
        return json.dumps(result)


class SumDataFrameJob(AnalyticsJob):

    name = 'sum_df_test_job'

    def main(self, a):
        result = {}
        result['sum'] = a.sum().sum()
        return json.dumps(result)

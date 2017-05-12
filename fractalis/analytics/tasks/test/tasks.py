import time

from fractalis.analytics.task import AnalyticTask


class AddJob(AnalyticTask):

    name = 'add_test_job'

    def main(self, a, b):
        result = {}
        result['sum'] = a + b
        return result


class DoNothingJob(AnalyticTask):

    name = 'nothing_test_job'

    def main(self, seconds):
        result = {}
        time.sleep(seconds)
        result['foo'] = 'bar'
        return result


class DivJob(AnalyticTask):

    name = 'div_test_job'

    def main(self, a, b):
        result = {}
        result['div'] = a / b
        return result


class SumDataFrameJob(AnalyticTask):

    name = 'sum_df_test_job'

    def main(self, a):
        result = {}
        result['sum'] = a.sum().sum()
        return result


class InvalidReturnJob(AnalyticTask):

    name = 'no_dict_job'

    def main(self):
        return 123


class InvalidJSONJob(AnalyticTask):
    name = 'invalid_json_job'

    def main(self):
        return {'a': lambda: 1}

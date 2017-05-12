import time

from fractalis.analytics.task import AnalyticTask


class AddTask(AnalyticTask):

    name = 'add_test_task'

    def main(self, a, b):
        result = {}
        result['sum'] = a + b
        return result


class DoNothingTask(AnalyticTask):

    name = 'nothing_test_task'

    def main(self, seconds):
        result = {}
        time.sleep(seconds)
        result['foo'] = 'bar'
        return result


class DivTask(AnalyticTask):

    name = 'div_test_task'

    def main(self, a, b):
        result = {}
        result['div'] = a / b
        return result


class SumDataFrameTask(AnalyticTask):

    name = 'sum_df_test_task'

    def main(self, a):
        result = {}
        result['sum'] = a.sum().sum()
        return result


class InvalidReturnTask(AnalyticTask):

    name = 'no_dict_task'

    def main(self):
        return 123


class InvalidJSONTask(AnalyticTask):
    name = 'invalid_json_task'

    def main(self):
        return {'a': lambda: 1}

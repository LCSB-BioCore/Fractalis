from time import sleep
from functools import reduce

from fractalis.analytics.task import AnalyticTask


class AddTask(AnalyticTask):

    name = 'add_test_task'

    def main(self, a, b):
        result = {'sum': a + b}
        return result


class DoNothingTask(AnalyticTask):

    name = 'nothing_test_task'

    def main(self, seconds):
        result = {}
        sleep(seconds)
        result['foo'] = 'bar'
        return result


class DivTask(AnalyticTask):

    name = 'div_test_task'

    def main(self, a, b):
        result = {'div': a / b}
        return result


class SumDataFrameTask(AnalyticTask):

    name = 'sum_df_test_task'

    def main(self, a):
        result = {'sum': a.sum().sum()}
        return result


class MergeDataFramesTask(AnalyticTask):

    name = 'merge_df_task'

    def main(self, df_list):
        if not df_list:
            return {'df': ''}
        df = reduce(lambda l, r: l.append(r), df_list)
        return {'df': df.to_json(orient='records')}


class InvalidReturnTask(AnalyticTask):

    name = 'no_dict_task'

    def main(self):
        return 123


class InvalidJSONTask(AnalyticTask):
    name = 'invalid_json_task'

    def main(self):
        return {'a': lambda: 1}

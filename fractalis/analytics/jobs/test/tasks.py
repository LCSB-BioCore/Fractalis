import time

from fractalis.analytics.job import AnalyticsJob


class AddJob(AnalyticsJob):

    name = 'add_test_job'

    def run(self, a, b):
        return a + b


class DoNothingJob(AnalyticsJob):

    name = 'nothing_test_job'

    def run(self, seconds):
        time.sleep(seconds)
        return 1


class DivJob(AnalyticsJob):

    name = 'div_test_job'

    def run(self, a, b):
        return a / b

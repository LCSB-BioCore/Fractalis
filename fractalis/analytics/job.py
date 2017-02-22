import abc

# TODO: is there a difference between this and importing
# fractalis.celery.app.Task ?
from celery import Task


class AnalyticsJob(Task, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def name(self):
        pass

    @staticmethod
    def factory(job_name):
        from . import JOB_REGISTRY
        for job in JOB_REGISTRY:
            if job.name == job_name:
                return job()

    @abc.abstractmethod
    def run(self):
        pass

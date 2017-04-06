import abc
import json

import pandas as pd
# TODO: is there a difference between this and importing
# fractalis.celeryapp.app.Task ?
from celery import Task

from fractalis import redis


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
    def main(self):
        pass

    def run(self, accessible_data_ids, **kwargs):
        args = {}
        for arg in kwargs:
            value = kwargs[arg]
            if (isinstance(value, str) and
                    value.startswith('$') and value.endswith('$')):
                data_id = value[1:-1]
                if data_id not in accessible_data_ids:
                    raise KeyError("No permission to use data_id '{}'"
                                   "for analysis".format(data_id))
                data_obj = redis.hget(name='data', key=data_id)
                if data_obj is None:
                    raise KeyError("The key '{}' does not match any entries"
                                   "in the database.".format(data_id))
                data_obj = json.loads(data_obj.decode('utf-8'))
                file_path = data_obj['file_path']
                value = pd.read_csv(file_path)
            args[arg] = value
        result = self.main(**args)
        try:
            json.loads(result)
        except Exception:
            raise ValueError("The job '{}' did not return valid JSON."
                             .format(self.name))
        return result

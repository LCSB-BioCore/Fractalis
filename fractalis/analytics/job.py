import abc
import json
import re

import pandas as pd
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

    @staticmethod
    def prepare_args(session_id, session_data_ids, args):
        arguments = {}
        for arg in args:
            value = args[arg]
            if (isinstance(value, str) and
                    value.startswith('$') and value.endswith('$')):
                data_id = value[1:-1]
                value = redis.get('data:{}'.format(data_id))
                if value is None:
                    raise KeyError("The key '{}' does not match any entries"
                                   " in the database.".format(data_id))
                data_obj = json.loads(value.decode('utf-8'))
                if session_id not in data_obj['access'] \
                        or data_id not in session_data_ids:  # access check
                    raise KeyError("No permission to use data_id '{}'"
                                   "for analysis".format(data_id))
                file_path = data_obj['file_path']
                value = pd.read_csv(file_path)
            arguments[arg] = value
        return arguments

    def run(self, session_id, session_data_ids, args):
        arguments = self.prepare_args(session_id, session_data_ids, args)
        result = self.main(**arguments)
        try:
            if type(result) != dict:
                raise ValueError("The job '{}' "
                                 "returned an object with type '{}', "
                                 "instead of expected type 'dict'.")
            result = json.dumps(result)
        except Exception:
            raise TypeError("The job '{}' result could not be JSON serialized."
                             .format(self.name))
        result = re.sub(r': NaN', ': null', result)
        return result

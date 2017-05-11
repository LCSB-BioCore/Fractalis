import abc
import json
import re
import logging
import time

import pandas as pd
from celery import Task

from fractalis import redis


logger = logging.getLogger(__name__)


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
    def prepare_args(accessible_data_ids, args):
        arguments = {}
        for arg in args:
            value = args[arg]
            if (isinstance(value, str) and
                    value.startswith('$') and value.endswith('$')):
                data_id = value[1:-1]
                if data_id not in accessible_data_ids:
                    error = "No permission to use data_id '{}'" \
                            "for analysis".format(data_id)
                    logger.error(error)
                    raise KeyError(error)
                entry = redis.get('data:{}'.format(data_id))
                if not entry:
                    error = "The key '{}' does not match any entry in Redis. " \
                            "Value probably expired.".format(data_id)
                    logger.error(error)
                    raise LookupError(error)
                data_obj = json.loads(entry.decode('utf-8'))
                # update 'last_access' internal
                data_obj['last_access'] = time.time()
                redis.set(name='data:{}'.format(data_id), value=data_obj)

                file_path = data_obj['file_path']
                value = pd.read_csv(file_path)
            arguments[arg] = value
        return arguments

    def run(self, accessible_data_ids, args):
        arguments = self.prepare_args(accessible_data_ids, args)
        result = self.main(**arguments)
        try:
            if type(result) != dict:
                error = "The job '{}' returned an object with type '{}', " \
                        "instead of expected type 'dict'."
                logger.error(error)
                raise ValueError(error)
            result = json.dumps(result)
        except TypeError as e:
            logging.exception(e)
            raise
        result = re.sub(r': NaN', ': null', result)
        return result

import abc
import json
import re
import logging
import time

import pandas as pd
from celery import Task

from fractalis import redis


logger = logging.getLogger(__name__)


class AnalyticTask(Task, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def name(self):
        pass

    @staticmethod
    def factory(task_name):
        from . import TASK_REGISTRY
        for task in TASK_REGISTRY:
            if task.name == task_name:
                return task()

    @abc.abstractmethod
    def main(self):
        pass

    @staticmethod
    def prepare_args(data_tasks, args):
        arguments = {}
        for arg in args:
            value = args[arg]
            if (isinstance(value, str) and
                    value.startswith('$') and value.endswith('$')):
                data_task_id = value[1:-1]

                if data_task_id not in data_tasks:
                    error = "No permission to use data_task_id '{}'" \
                            "for analysis".format(data_task_id)
                    logger.error(error)
                    raise PermissionError(error)
                entry = redis.get('data:{}'.format(data_task_id))
                if not entry:
                    error = "The key '{}' does not match any entry in Redis. " \
                            "Value probably expired.".format(data_task_id)
                    logger.error(error)
                    raise LookupError(error)
                data_state = json.loads(entry.decode('utf-8'))
                if not data_state['loaded']:
                    error = "The data task '{}' has not been loaded, yet." \
                            "Wait for it to complete before using it in an " \
                            "analysis task.".format(data_task_id)
                    logger.error(error)
                    raise ValueError(error)
                file_path = data_state['file_path']
                value = pd.read_csv(file_path)
            arguments[arg] = value
        return arguments

    def run(self, data_tasks, args):
        arguments = self.prepare_args(data_tasks, args)
        result = self.main(**arguments)
        try:
            if type(result) != dict:
                error = "The task '{}' returned an object with type '{}', " \
                        "instead of expected type 'dict'."
                logger.error(error)
                raise ValueError(error)
            result = json.dumps(result)
        except TypeError as e:
            logging.exception(e)
            raise
        result = re.sub(r': NaN', ': null', result)
        return result

"""This module provides AnalyticTask, which is a modification of a standard
Celery task tailored to Fractalis."""
import abc
import json
import re
import logging
from typing import List

import pandas as pd
from celery import Task

from fractalis import redis


logger = logging.getLogger(__name__)


class AnalyticTask(Task, metaclass=abc.ABCMeta):
    """AnalyticTask is a tailored Celery Task that enforces a certain pattern
    for all Fractalis analytic tasks and provides certain functionality like
    the parsing of the arguments before submitting it to celery.
    """
    @property
    @abc.abstractmethod
    def name(self) -> str:
        """The name of the task."""
        pass

    @staticmethod
    def factory(task_name: str) -> 'AnalyticTask':
        """Initialize the correct task based on the given arguments.
        :param task_name: The name of the task to initialize.
        :return: An initialized child of this class.
        """
        from . import TASK_REGISTRY
        for task in TASK_REGISTRY:
            if task.name == task_name:
                return task()

    @abc.abstractmethod
    def main(self) -> dict:
        """Since we hijack run(), we need a new entry point for every task.
        This method is called by our modified run method with all parsed 
        arguments.
        :return A dict containing the results of the task.
        """
        pass

    @staticmethod
    def prepare_args(data_tasks: List[str], args: dict) -> dict:
        """Replace data task ids in the arguments with their associated 
        data frame located on the file system.

        :param data_tasks: We use this list to check access.
        :param args: The arguments submitted to run()
        :return: The new parsed arguments
        """
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
                data_state = json.loads(entry)
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

    def run(self, data_tasks: List[str], args: dict) -> str:
        """This is called by the celery worker. This method calls other helper
        methods to prepare and validate the in and output of a task.
        :param data_tasks: List of data task ids from session to check access.
        :param args: This is the dict of arguments submitted to the task.
        :return: The result of the task.
        """
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

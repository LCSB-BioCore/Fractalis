"""This module provides AnalyticTask, which is a modification of a standard
Celery task tailored to Fractalis."""
import abc
import json
import re
import logging
from typing import List

from pandas import read_csv, DataFrame
from celery import Task
from Crypto.Cipher import AES

from fractalis import redis, app
from fractalis.utils import get_cache_encrypt_key

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
    def main(self, *args, **kwargs) -> dict:
        """Since we hijack run(), we need a new entry point for every task.
        This method is called by our modified run method with all parsed
        arguments.
        :return A dict containing the results of the task.
        """
        pass

    def secure_load(self, file_path: str) -> DataFrame:
        """Decrypt data so they can be loaded into a pandas data frame.
        :param file_path: The location of the encrypted file.
        :return: The decrypted file loaded into a pandas data frame.
        """
        key = get_cache_encrypt_key(app.config['SECRET_KEY'])
        with open(file_path, 'rb') as f:
            nonce, tag, ciphertext = [f.read(x) for x in (16, 16, -1)]
        cipher = AES.new(key, AES.MODE_EAX, nonce)
        data = cipher.decrypt_and_verify(ciphertext, tag)
        data = data.decode('utf-8')
        data = json.loads(data)
        df = DataFrame.from_dict(data)
        return df

    def data_task_id_to_data_frame(self, data_task_id,
                                   session_data_tasks, decrypt):
        """Attempts to load the data frame associated with the provided data id.
        :param data_task_id: The data id associated with the previously loaded
        data.
        :param session_data_tasks: A list of data tasks previously executed by
        this the requesting session. This is used for permission checks.
        :param decrypt: Specify whether the data have to be decrypted for usage.
        :return: A pandas data frame associated with the data id.
        """
        if data_task_id not in session_data_tasks:
            error = "No permission to use data_task_id '{}' " \
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
            error = "The data task '{}' has not been loaded, yet. " \
                    "Wait for it to complete before using it in an " \
                    "analysis task.".format(data_task_id)
            logger.error(error)
            raise ValueError(error)
        file_path = data_state['file_path']
        if decrypt:
            df = self.secure_load(file_path)
        else:
            df = read_csv(file_path)
        df = self.load_data_frame(file_path, decrypt)
        return df

    def prepare_args(self, session_data_tasks: List[str],
                     args: dict, decrypt: bool) -> dict:
        """Replace data task ids in the arguments with their associated 
        data frame located on the file system. This currently works for non
        nested strings and non nested lists containing strings.
        :param session_data_tasks: We use this list to check access.
        :param args: The arguments submitted to run().
        :param decrypt: Indicates whether cache must be decrypted to be used.
        :return: The new parsed arguments
        """
        arguments = {}
        for arg in args:
            value = args[arg]
            # value is data id
            if (isinstance(value, str) and value and
                    value.startswith('$') and value.endswith('$')):
                data_task_id = value[1:-1]
                value = self.data_task_id_to_data_frame(
                    data_task_id, session_data_tasks, decrypt)
            # value is list containing data ids
            if (isinstance(value, list) and value and
                    value[0].startswith('$') and value[0].endswith('$')):
                data_task_ids = [el[1:-1] for el in value]
                dfs = []
                for data_task_id in data_task_ids:
                    df = self.data_task_id_to_data_frame(
                        data_task_id, session_data_tasks, decrypt)
                    dfs.append(df)
                value = dfs
            arguments[arg] = value
        return arguments

    def task_result_to_json(self, result: dict) -> str:
        """Transform task result to JSON so we can send it as a response.
        :param result: The return value of main()
        :return: A string that can be parsed by the front-end for instance.
        """
        try:
            if type(result) != dict:
                error = "The task '{}' returned an object with type '{}', " \
                        "instead of expected type 'dict'."
                logger.error(error)
                raise ValueError(error)
            result = json.dumps(result)
        except TypeError as e:
            logger.exception(e)
            raise
        # NaN is invalid JSON and JS can't parse it. null on the other hand...
        result = re.sub(r': NaN', ': null', result)
        return result

    def run(self, session_data_tasks: List[str],
            args: dict, decrypt: bool) -> str:
        """This is called by the celery worker. This method calls other helper
        methods to prepare and validate the in and output of a task.
        :param session_data_tasks: List of data task ids from session to check
        access.
        :param args: The dict of arguments submitted to the task.
        :param decrypt: Indicates whether cache must be decrypted to be used.
        :return: The result of the task.
        """
        arguments = self.prepare_args(session_data_tasks, args, decrypt)
        result = self.main(**arguments)
        json = self.task_result_to_json(result)
        return json

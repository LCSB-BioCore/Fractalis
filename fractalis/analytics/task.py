"""This module provides AnalyticTask, which is a modification of a standard
Celery task tailored to Fractalis."""
import abc
import json
import re
import logging
from uuid import UUID
from typing import List, Tuple, Union

from pandas import read_pickle, DataFrame
from celery import Task
from Cryptodome.Cipher import AES

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

    @staticmethod
    def secure_load(file_path: str) -> DataFrame:
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

    def data_task_id_to_data_frame(
            self, data_task_id: str,
            session_data_tasks: List[str], decrypt: bool) -> DataFrame:
        """Attempts to load the data frame associated with the provided data id
        :param data_task_id: The data id associated with the previously loaded
        data.
        :param session_data_tasks: A list of data tasks previously executed by
        this the requesting session. This is used for permission checks.
        :param decrypt: Specify whether the data have to be decrypted for usage
        only part of the data, for instance some genes out of thousands.
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
        async_result = self.AsyncResult(data_task_id)
        if async_result.state != 'SUCCESS':
            error = "The data task '{}' has not been loaded, yet. " \
                    "Wait for it to complete before using it in an " \
                    "analysis task.".format(data_task_id)
            logger.error(error)
            raise ValueError(error)
        file_path = data_state['file_path']
        if decrypt:
            return self.secure_load(file_path)
        else:
            df = read_pickle(file_path, compression='gzip')
        return df

    @staticmethod
    def apply_filters(df: DataFrame, filters: dict) -> DataFrame:
        """Apply filter to data frame and return it.
        :param df: The data frame.
        :param filters: The filters where each key is represents a column
        in the data frame and the value a list of values to keep.
        :return: Filtered data frame.
        """
        for key in filters:
            if filters[key]:
                df = df[df[key].isin(filters[key])]
        return df

    @staticmethod
    def contains_data_task_id(value: str) -> bool:
        """Check whether the given string represents
        a special data id arguments.
        :param value: The string to test.
        :return: True if argument contains data_task_id.
        """
        return isinstance(value, str) and \
            value.startswith('$') and \
            value.endswith('$')

    @staticmethod
    def parse_value(value: str) -> Tuple[Union[str, None], dict]:
        """Extract data task id and filters from the string.
        :param value: A string that contains a data task id.
        :return: A tuple of id and filters to apply later.
        """
        value = value[1:-1]
        # noinspection PyBroadException
        try:
            value = json.loads(value)
            data_task_id = str(value['id'])
            filters = value.get('filters')
        except Exception:
            logger.warning("Failed to parse value. "
                           "Fallback assumption is that it contains the id "
                           "but nothing else.")
            data_task_id = value
            filters = None
        # noinspection PyBroadException
        try:
            data_task_id = str(UUID(data_task_id))
        except Exception:
            logger.warning("'{}' is no valid task id.".format(data_task_id))
            data_task_id = None
        return data_task_id, filters

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
        parsed_args = {}
        for arg in args:
            value = args[arg]

            # value is data id
            if self.contains_data_task_id(value):
                data_task_id, filters = self.parse_value(value)
                df = self.data_task_id_to_data_frame(
                    data_task_id, session_data_tasks, decrypt)
                if filters:
                    df = self.apply_filters(df, filters)
                value = df

            # value is list containing data ids
            if (isinstance(value, list) and
                    value and self.contains_data_task_id(value[0])):
                dfs = []
                for el in value:
                    data_task_id, filters = self.parse_value(el)
                    df = self.data_task_id_to_data_frame(
                        data_task_id, session_data_tasks, decrypt)
                    if filters:
                        df = self.apply_filters(df, filters)
                    dfs.append(df)
                value = dfs

            parsed_args[arg] = value

        return parsed_args

    @staticmethod
    def task_result_to_json(result: dict) -> str:
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
        result = re.sub(r'NaN', 'null', result)
        return result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        """Set lifetime of analysis result to prevent redis from consuming
        too much memory."""
        redis.expire(name='celery-task-meta-{}'.format(task_id),
                     time=app.config['FRACTALIS_RESULT_LIFETIME'])

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

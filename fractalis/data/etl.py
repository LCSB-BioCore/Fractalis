"""This module provides the ETL class"""

import os
import abc
import json
import logging
from typing import List

from celery import Task
from pandas import DataFrame

from fractalis import app, redis


logger = logging.getLogger(__name__)


class ETL(Task, metaclass=abc.ABCMeta):
    """This is an abstract class that implements a celery Task and provides a
    factory method to create instances of implementations of itself. Its main
    purpose is to  manage extraction of the data from the target server. ETL
    stands for (E)xtract (T)ransform (L)oad and not by coincidence similar named
    methods can be found in this class.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Used by celery to identify this task by name."""
        pass

    @property
    @abc.abstractmethod
    def _handler(self) -> str:
        """Used by self.can_handle to check whether the current implementation
        belongs to a certain handler. This is necessary to avoid conflicts with
        other ETL with identical self.name field.
        """
        pass

    @property
    @abc.abstractmethod
    def _accepts(self) -> List[str]:
        """Used by self.can_handle to check whether the current implementation
        can handle the given data type. One ETL can handle multiple data types,
        therefor this is a list.
        """
        pass

    @property
    @abc.abstractmethod
    def produces(self) -> str:
        """This specifies the fractalis internal format that this ETL
        produces. Can be one of: ['categorical', 'numerical']
        """
        pass

    @classmethod
    def can_handle(cls, handler: str, data_type: str) -> bool:
        """Check if the current implementation of ETL can handle given handler
        and data type.
        :param handler: Describes the handler. E.g.: transmart, ada
        :param data_type: Describes the data type. E.g.: ldd, hdd
        :return: True if implementation can handle given parameters.
        """
        return handler == cls._handler and data_type == cls._accepts

    @classmethod
    def factory(cls, handler: str, data_type: str) -> 'ETL':
        """Return an instance of the implementation ETL that can handle the
        given parameters.
        :param handler: Describes the handler. E.g.: transmart, ada
        :param data_type: Describes the data type. E.g.: ldd, hdd
        :return: An instance of an implementation of ETL that returns True for
        self.can_handle
        """
        from . import ETL_REGISTRY
        for etl in ETL_REGISTRY:
            if etl.can_handle(handler, data_type):
                return etl()
        raise NotImplementedError(
            "No ETL implementation found for handler '{}' and data type '{}'"
            .format(handler, data_type))

    @abc.abstractmethod
    def extract(self, server: str, token: str, descriptor: dict) -> object:
        """Extract the data via HTTP requests.
        :param server: The server from which to extract from.
        :param token: The token used for authentication.
        :param descriptor: The descriptor containing all necessary information
        to download the data.
        """
        pass

    @abc.abstractmethod
    def transform(self, raw_data: object) -> DataFrame:
        """Transform the data into a pandas.DataFrame with a naming according to
        the Fractalis standard format.
        :param raw_data: The data to transform.
        """
        pass

    def load(self, data_frame: DataFrame, file_path: str) -> None:
        """Load (save) the data to the file system.
        :param data_frame: DataFrame to write.
        :param file_path: File to write to.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        data_frame.to_csv(file_path, index=False)
        value = redis.get(name='data:{}'.format(self.request.id))
        assert value is not None
        data_state = json.loads(value)
        data_state['loaded'] = True
        redis.setex(name='data:{}'.format(self.request.id),
                    value=json.dumps(data_state),
                    time=app.config['FRACTALIS_CACHE_EXP'])

    def run(self, server: str, token: str,
            descriptor: dict, file_path: str) -> None:
        """Run extract, transform and load. This is called by the celery worker.
        This is called by the celery worker.
        :param
        :param server: The server on which the data are located.
        :param token: The token used for authentication.
        :param descriptor: Contains all necessary information to download data
        :param file_path: The location where the data will be stored
        :return: The data id. Used to access the associated redis entry later on
        """
        logger.info("Starting ETL process ...")
        logger.info("(E)xtracting data from server '{}'.".format(server))
        raw_data = self.extract(server, token, descriptor)
        logger.info("(T)ransforming data to Fractalis format.")
        data_frame = self.transform(raw_data)
        if not isinstance(data_frame, DataFrame):
            error = "transform() must return 'pandas.DataFrame', " \
                    "but returned '{}' instead.".format(type(data_frame))
            logging.error(error, exc_info=1)
            raise TypeError(error)
        self.load(data_frame, file_path)

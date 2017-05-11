"""This module provides the ETL class"""

import os
import abc
import json
import time
import logging
from typing import List
from hashlib import sha256

from celery import Task
from pandas import DataFrame

from fractalis import redis


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

    @staticmethod
    def load(data_frame: DataFrame, file_path: str) -> None:
        """Load (save) the data to the file system.
        :param data_frame: DataFrame to write.
        :param file_path: File to write to.
        """
        data_frame.to_csv(file_path, index=False)

    @staticmethod
    def compute_data_id(server: str, descriptor: dict) -> str:
        """Return a hash key based on the given parameters.
        Parameters are automatically sorted before the hash is computed.
        :param server: The server which is being handled.
        :param descriptor: A dict describing the data.
        :return: The computed hash key.
        """
        descriptor_str = json.dumps(descriptor, sort_keys=True)
        to_hash = '{}|{}'.format(server, descriptor_str).encode('utf-8')
        hash_key = sha256(to_hash).hexdigest()
        return hash_key

    @classmethod
    def create_redis_entry(cls, data_id: str,
                           file_path: str, descriptor: dict) -> None:
        """Creates an entry in Redis that reflects all meta data surrounding the
        downloaded data. E.g. last access, data type, file system location, ...
        :param data_id: Id associated with the loaded data. 
        :param file_path: Location of the data on the file system
        :param descriptor: Describes the data and is used to download them
        """
        try:
            label = descriptor['label']
        except KeyError:
            label = str(descriptor)
        data_obj = {
            'file_path': file_path,
            'last_access': time.time(),
            'label': label,
            'descriptor': descriptor,
            'data_type': cls.produces
        }
        redis.set(name='data:{}'.format(data_id),
                  value=json.dumps(data_obj))

    def run(self, server: str, token: str,
            descriptor: dict, tmp_dir: str) -> str:
        """Run the current task, that is running extract, transform and load.
        This is called by the celery worker.
        :param server: The server on which the data are located.
        :param token: The token used for authentication.
        :param descriptor: Contains all necessary information to download data
        :param tmp_dir: The directory where the files are stored 
        :return: The data id. Used to access the associated redis entry later on
        """
        logger.info("Starting ETL process ...")
        data_id = self.compute_data_id(server, descriptor)
        data_dir = os.path.join(tmp_dir, 'data')
        os.makedirs(data_dir, exist_ok=True)
        file_path = os.path.join(data_dir, data_id)
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
        self.create_redis_entry(data_id, file_path, descriptor)
        return data_id

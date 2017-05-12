"""This module provides the ETLHandler class."""

import os
import abc
import json
import logging
from uuid import uuid4
from typing import List


from fractalis import app, redis
from fractalis.data.etl import ETL


logger = logging.getLogger(__name__)


class ETLHandler(metaclass=abc.ABCMeta):
    """This is an abstract class that provides a factory method to create
    instances of implementations of itself. The main purpose of this class
    is the supervision of all ETL processes belonging to this handler. Besides
    that it takes care of authentication business.
    """

    @property
    @abc.abstractmethod
    def _handler(self) -> str:
        """Used by self.can_handle to check whether the current implementation
        is able to handle the incoming request.
        """
        pass

    def _get_token_for_credentials(self, server: str,
                                   user: str, passwd: str) -> str:
        """ Authenticate with the server and return a token.
        :param server: The server to authenticate with.
        :param user: The user id.
        :param passwd: The password.
        """
        raise NotImplementedError()

    def __init__(self, server, auth):
        self._server = server
        # if no token is given we have to get one
        try:
            self._token = auth['token']
        except KeyError:
            self._token = self._get_token_for_credentials(
                server, auth['user'], auth['passwd'])

    @classmethod
    def create_redis_entry(cls, task_id: str, file_path: str,
                           descriptor: dict, data_type: str) -> None:
        """Creates an entry in Redis that reflects all meta data surrounding the
        downloaded data. E.g. data type, file system location, ...
        :param task_id: Id associated with the loaded data. 
        :param file_path: Location of the data on the file system
        :param descriptor: Describes the data and is used to download them
        :param data_type: The fractalis internal data type of the loaded data
        """
        try:
            label = descriptor['label']
        except KeyError:
            label = str(descriptor)
        data_state = {
            'task_id': task_id,
            'file_path': file_path,
            'label': label,
            'descriptor': descriptor,
            'data_type': data_type,
            'loaded': False
        }
        redis.set(name='data:{}'.format(task_id),
                  value=json.dumps(data_state))

    def handle(self, descriptors: List[dict], wait: bool = False) -> List[str]:
        """Create instances of ETL for the given descriptors and submit them
        (ETL implements celery.Task) to the broker. The task ids are returned to
        keep track of them.
        :param descriptors: A list of items describing the data to download.
        :param wait: Makes this method synchronous by waiting for the tasks to
        return.
        :return: The list of task ids for the submitted tasks.
        """
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        task_ids = []
        for descriptor in descriptors:
            task_id = str(uuid4())
            file_path = os.path.join(data_dir, task_id)
            etl = ETL.factory(handler=self._handler,
                              data_type=descriptor['data_type'])
            self.create_redis_entry(task_id, file_path,
                                    descriptor, etl.produces)
            kwargs = dict(server=self._server, token=self._token,
                          descriptor=descriptor, file_path=file_path)
            async_result = etl.apply_async(kwargs=kwargs, task_id=task_id)
            task_ids.append(async_result.id)
            if wait:
                logger.debug("'wait' was set. Waiting for tasks to finish ...")
                async_result.get(propagate=False)
        return task_ids

    @classmethod
    def factory(cls, handler: str, server: str, auth: dict) -> 'ETLHandler':
        """Return an instance of the implementation of ETLHandler that can
        handle the given parameters.
        :param handler: Describes the handler. E.g.: transmart, ada
        :param server: The server to download data from.
        :param auth: Contains credentials to authenticate with the API.
        :return: An instance of an implementation of ETLHandler that returns
        True for self.can_handle
        """
        from . import HANDLER_REGISTRY
        for Handler in HANDLER_REGISTRY:
            if Handler.can_handle(handler):
                return Handler(server, auth)
        raise NotImplementedError(
            "No ETLHandler implementation found for: '{}'".format(handler))

    @classmethod
    def can_handle(cls, handler: str) -> bool:
        """Check whether this implementation of ETLHandler can handle the given
        parameters.
        :param handler: Describes the handler. E.g.: transmart, ada
        :return: True if this implementation can handle the given parameters.
        """
        return handler == cls._handler

    @abc.abstractmethod
    def _heartbeat(self):
        pass

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

    def _get_token_for_credentials(self, server: str, auth: dict) -> str:
        """ Authenticate with the server and return a token.
        :param server: The server to authenticate with.
        :param auth: dict containing credentials to auth with API
        :return The token returned by the API.
        """
        raise NotImplementedError()

    def __init__(self, server, auth):
        if not isinstance(server, str) or len(server) < 10:
            error = ("{} is not a valid server url.".format(server))
            logger.error(error)
            raise ValueError(error)
        if not isinstance(auth, dict):
            error = "'auth' must be a valid dictionary."
            logger.error(error)
            raise ValueError(error)
        if server.endswith('/'):
            server = server[:-1]
        self._server = server
        # if no token is given we have to get one
        try:
            self._token = auth['token']
            if not isinstance(self._token, str) or len(self._token) == 0:
                raise KeyError
        except KeyError:
            logger.info('No token has been provided. '
                        'Attempting to authenticate with the API.')
            try:
                self._token = self._get_token_for_credentials(server, auth)
            except Exception as e:
                logger.exception(e)
                raise ValueError("Could not authenticate with API.")

    @staticmethod
    @abc.abstractmethod
    def make_label(descriptor: dict) -> str:
        """Create a label for the given data descriptor. This label will be
        used to display the loaded data in the front end.
        :param descriptor: Describes the data and is used to download them.
        :return The label (not necessarily unique) to the data."""
        pass

    def create_redis_entry(self, task_id: str, file_path: str,
                           descriptor: dict, data_type: str) -> None:
        """Creates an entry in Redis that reflects all meta data surrounding the
        downloaded data. E.g. data type, file system location, ...
        :param task_id: Id associated with the loaded data.
        :param file_path: Location of the data on the file system.
        :param descriptor: Describes the data and is used to download them.
        :param data_type: The fractalis internal data type of the loaded data.
        """
        data_state = {
            'task_id': task_id,
            'file_path': file_path,
            'label': self.make_label(descriptor),
            'data_type': data_type,
            'meta': {
                'descriptor': descriptor,
            },
            'loaded': False,
        }
        redis.setex(name='data:{}'.format(task_id),
                    value=json.dumps(data_state),
                    time=app.config['FRACTALIS_CACHE_EXP'])

    def handle(self, descriptors: List[dict], wait: bool = False) -> List[str]:
        """Create instances of ETL for the given descriptors and submit them
        (ETL implements celery.Task) to the broker. The task ids are returned
        to keep track of them.
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
            etl = ETL.factory(handler=self._handler, descriptor=descriptor)
            self.create_redis_entry(task_id, file_path,
                                    descriptor, etl.produces)
            kwargs = dict(server=self._server, token=self._token,
                          descriptor=descriptor, file_path=file_path,
                          encrypt=app.config['FRACTALIS_ENCRYPT_CACHE'])
            async_result = etl.apply_async(kwargs=kwargs, task_id=task_id)
            assert async_result.id == task_id
            task_ids.append(task_id)
            if wait:
                logger.debug("'wait' was set. Waiting for tasks to finish ...")
                async_result.get(propagate=False)
        return task_ids

    @staticmethod
    def factory(handler: str, server: str, auth: dict) -> 'ETLHandler':
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

    def _heartbeat(self):
        raise NotImplementedError()

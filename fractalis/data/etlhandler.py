"""This module provides the ETLHandler class."""

import os
import abc
import json
import time
from hashlib import sha256
from uuid import uuid4
from typing import List

from fractalis.data.etl import ETL
from fractalis import app
from fractalis import redis


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

    @abc.abstractmethod
    def _get_token_for_credentials(self, server: str,
                                   user: str, passwd: str) -> str:
        """ Authenticate with the server and return a token.

        :param server: The server to authenticate with.
        :param user: The user id.
        :param passwd: The password.
        """
        pass

    def __init__(self, server, auth):
        self._server = server
        # if no token is given we have to get one
        try:
            self._token = auth['token']
        except KeyError:
            self._token = self._get_token_for_credentials(
                server, auth['user'], auth['passwd'])

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

    def handle(self, descriptors: List[dict], wait: bool = False) -> List[str]:
        """Create instances of ETL for the given descriptors and submit them
        (ETL implements celery.Task) to the broker. The task ids are returned to
        keep track of them.

        :param descriptors: A list of items describing the data to download.
        :param wait: Makes this method synchronous by waiting for the tasks to
        return.
        :return: The list of task ids for the submitted tasks.
        """
        data_ids = []
        for descriptor in descriptors:
            data_id = self.compute_data_id(self._server, descriptor)
            tmp_dir = app.config['FRACTALIS_TMP_DIR']
            data_dir = os.path.join(tmp_dir, 'data')
            os.makedirs(data_dir, exist_ok=True)
            value = redis.hget('data', key=data_id)
            if value:  # if value exists use existing file path
                file_path = json.loads(value.decode('utf-8'))['file_path']
            else:
                file_name = str(uuid4())
                file_path = os.path.join(data_dir, file_name)
            etl = ETL.factory(handler=self._handler,
                              data_type=descriptor['data_type'])
            async_result = etl.delay(server=self._server,
                                     token=self._token,
                                     descriptor=descriptor,
                                     file_path=file_path)
            if wait:
                async_result.get(propagate=False)  # wait for results
            data_obj = {
                'file_path': file_path,
                'job_id': async_result.id,
                'last_access': time.time(),
                'data_type': etl.produces
            }
            redis.hset(name='data', key=data_id, value=json.dumps(data_obj))
            data_ids.append(data_id)
        return data_ids

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

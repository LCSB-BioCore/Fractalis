import os
import abc
import json
from hashlib import sha256
from uuid import uuid4

from fractalis.data.etls.etl import ETL
from fractalis import app
from fractalis import redis


class ETLHandler(metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def _HANDLER(self):
        pass

    def __init__(self, server, token):
        self._server = server
        self._token = token

    @staticmethod
    def compute_data_id(server, descriptor):
        descriptor_str = json.dumps(descriptor, sort_keys=True)
        to_hash = '{}|{}'.format(server, descriptor_str).encode('utf-8')
        hash_key = sha256(to_hash).hexdigest()
        return hash_key

    def handle(self, descriptors):
        data_ids = []
        for descriptor in descriptors:
            data_id = self.compute_data_id(self._server, descriptor)
            tmp_dir = app.config['FRACTALIS_TMP_DIR']
            data_dir = os.path.join(tmp_dir, 'data')
            os.makedirs(data_dir, exist_ok=True)
            file_name = str(uuid4())
            file_path = os.path.join(data_dir, file_name)
            etl = ETL.factory(handler=self._HANDLER,
                              data_type=descriptor['data_type'])
            async_result = etl.delay(server=self._server,
                                     token=self._token,
                                     descriptor=descriptor,
                                     file_path=file_path)
            data_obj = {'file_path': file_path, 'job_id': async_result.id}
            redis.hset(name='data', key=data_id, value=json.dumps(data_obj))
            data_ids.append(data_id)
        return data_ids

    @classmethod
    def factory(cls, handler, server, token):
        from . import HANDLER_REGISTRY
        for Handler in HANDLER_REGISTRY:
            if Handler.can_handle(handler):
                return Handler(server, token)
        raise NotImplementedError(
            "No ETLHandler implementation found for: '{}'".format(handler))

    @classmethod
    def can_handle(cls, handler):
        return handler == cls._HANDLER

    @abc.abstractmethod
    def _heartbeat(self):
        pass

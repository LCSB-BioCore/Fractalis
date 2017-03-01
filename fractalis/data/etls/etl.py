import os
import abc
import json
from uuid import uuid4
from hashlib import sha256

from celery import Task
from pandas import DataFrame

from fractalis import app
from fractalis import redis


class ETL(Task, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def name(self):
        pass

    @property
    @abc.abstractmethod
    def _HANDLER(self):
        pass

    @property
    @abc.abstractmethod
    def _DATA_TYPE(self):
        pass

    @classmethod
    def can_handle(cls, handler, data_type):
        return handler == cls._HANDLER and data_type == cls._DATA_TYPE

    @classmethod
    def factory(cls, handler, data_type):
        from . import ETL_REGISTRY
        for etl in ETL_REGISTRY:
            if etl.can_handle(handler, data_type):
                return etl()
        raise NotImplementedError(
            "No ETL implementation found for handler '{}'' and data type '{}'"
            .format(handler, data_type))

    @abc.abstractmethod
    def extract(self, server, token, descriptor):
        pass

    @abc.abstractmethod
    def transform(self, raw_data):
        pass

    def load(self, data_frame, server, descriptor):
        data_dir = app.config['FRACTALIS_TMP_FOLDER']
        os.makedirs(data_dir, exist_ok=True)
        file_name = uuid4()
        file_path = os.path.join(data_dir, file_name)
        descriptor_str = json.dumps(descriptor, sort_keys=True)
        to_hash = '{}|{}'.format(server, descriptor_str).encode('utf-8')
        hash_key = sha256(to_hash)
        data_frame.to_csv(file_path)
        redis.hset(name='data', key=hash_key, value=file_path)

    def run(self, server, token, descriptor):
        raw_data = self.extract(server, token, descriptor)
        data_frame = self.transform(raw_data)
        if not isinstance(data_frame, DataFrame):
            raise TypeError("transform() must return 'pandas.DataFrame', but"
                            "returned '{}' instead.".format(type(data_frame)))
        self.load(data_frame, server, descriptor)

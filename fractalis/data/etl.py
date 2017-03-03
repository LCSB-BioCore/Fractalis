import abc

from celery import Task
from pandas import DataFrame


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
            "No ETL implementation found for handler '{}' and data type '{}'"
            .format(handler, data_type))

    @abc.abstractmethod
    def extract(self, server, token, descriptor):
        pass

    @abc.abstractmethod
    def transform(self, raw_data):
        pass

    def load(self, data_frame, file_path):
        data_frame.to_csv(file_path)

    def validate_state(self):
        return True

    def run(self, server, token, descriptor, file_path):
        raw_data = self.extract(server, token, descriptor)
        data_frame = self.transform(raw_data)
        if not isinstance(data_frame, DataFrame):
            raise TypeError("transform() must return 'pandas.DataFrame', but"
                            "returned '{}' instead.".format(type(data_frame)))
        self.load(data_frame, file_path)
        self.validate_state()

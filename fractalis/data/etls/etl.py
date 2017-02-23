import abc

# TODO: is there a difference between this and importing
# fractalis.celery.app.Task ?
from celery import Task


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
    def extract(self, descriptor):
        pass

    @abc.abstractmethod
    def transform(self, raw_data):
        pass

    def load(self, data):
        pass

    def run(self, descriptor):
        raw_data = self.extract(descriptor)
        data = self.transform(raw_data)
        self.load(data)

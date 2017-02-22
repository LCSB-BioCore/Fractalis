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
    def can_handle(cls, params):
        return (params['_handler'] == cls._HANDLER and
                params['_descriptor']['data_type'] == cls._DATA_TYPE)

    @classmethod
    def factory(cls, params):
        from . import ETL_REGISTRY
        for etl in ETL_REGISTRY:
            if etl.can_handle(params):
                return etl()
        raise NotImplementedError(
            "No ETL implementation found for: '{}'"
            .format(params))

    @abc.abstractmethod
    def run(self):
        pass

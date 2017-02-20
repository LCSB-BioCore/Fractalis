import abc
from fractalis.celery import app as celery


class ETL(celery.Task, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def _HANDLER(self):
        pass

    @property
    @abc.abstractmethod
    def _DATA_TYPE(self):
        pass

    def __init__(self, params):
        for key in params:
            self.__dict__[key] = params[key]

    @classmethod
    def _can_handle(cls, params):
        return (params['_handler'] == cls._HANDLER and
                params['_descriptor']['data_type'] == cls._DATA_TYPE)

    @classmethod
    def factory(cls, params):
        for subclass in cls.__subclasses__():
            if subclass._can_handle(params):
                return subclass(params)
        raise NotImplemented(
            "No ETL implementation found for: '{}'"
            .format(params))

    @abc.abstractmethod
    def run(self):
        pass

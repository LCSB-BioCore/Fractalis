import abc
import copy

from fractalis.data.etls.etl import ETL


class ETLHandler(metaclass=abc.ABCMeta):

    _etls = []

    @property
    @abc.abstractmethod
    def _HANDLER(self):
        pass

    def __init__(self, kwargs):
        for key in kwargs:
            self.__dict__['_' + key] = kwargs[key]

    def handle(self):
        assert not self._etls
        assert self._descriptors
        for descriptor in self._descriptors:
            params = copy.deepcopy(vars(self))
            del params['_descriptors']
            params['_descriptor'] = descriptor
            etl = ETL.factory(params)
            self._etls.append(etl)

    @classmethod
    def factory(cls, **kwargs):
        for subclass in cls.__subclasses__():
            if subclass._can_handle(kwargs):
                return subclass(kwargs)
        raise NotImplemented(
            "No ETLHandler implementation found for: '{}'"
            .format(kwargs))

    @classmethod
    def _can_handle(cls, kwargs):
        return kwargs['handler'] == cls._HANDLER

    @abc.abstractmethod
    def _heartbeat(self):
        pass

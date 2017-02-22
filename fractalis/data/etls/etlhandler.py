import abc
import copy

from fractalis.data.etls.etl import ETL


class ETLHandler(metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def _HANDLER(self):
        pass

    def __init__(self, kwargs):
        for key in kwargs:
            self.__dict__['_' + key] = kwargs[key]

    def handle(self):
        assert self._descriptors
        etl_job_ids = []
        for descriptor in self._descriptors:
            params = copy.deepcopy(vars(self))
            del params['_descriptors']
            params['_descriptor'] = descriptor
            etl = ETL.factory(params)
            async_result = etl.delay(params)
            etl_job_ids.append(async_result.id)
        return etl_job_ids

    @classmethod
    def factory(cls, **kwargs):
        from . import HANDLER_REGISTRY
        for handler in HANDLER_REGISTRY:
            if handler.can_handle(kwargs):
                return handler(kwargs)
        raise NotImplementedError(
            "No ETLHandler implementation found for: '{}'"
            .format(kwargs))

    @classmethod
    def can_handle(cls, kwargs):
        return kwargs['handler'] == cls._HANDLER

    @abc.abstractmethod
    def _heartbeat(self):
        pass

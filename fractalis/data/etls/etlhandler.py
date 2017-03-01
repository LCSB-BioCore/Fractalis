import abc

from fractalis.data.etls.etl import ETL


class ETLHandler(metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def _HANDLER(self):
        pass

    def __init__(self, server, token):
        self._server = server
        self._token = token

    def handle(self, descriptors):
        etl_job_ids = []
        for descriptor in descriptors:
            etl = ETL.factory(handler=self._HANDLER,
                              data_type=descriptor['data_type'])
            async_result = etl.delay(server=self._server,
                                     token=self._token,
                                     descriptor=descriptor)
            etl_job_ids.append(async_result.id)
        return etl_job_ids

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

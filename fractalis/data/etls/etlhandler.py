import abc


class ETLHandler(metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def _ETL(self):
        pass

    def can_handle(self, etl):
        return etl == self._ETL

    def handle(self):
        pass

    @staticmethod
    def factory(self, etl):
        for subclass in self.__subclasses__():
            if subclass.can_handle(etl):
                return subclass()

    @abc.abstractmethod
    def heartbeat(self):
        pass

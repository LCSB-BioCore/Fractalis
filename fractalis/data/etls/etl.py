import abc


class ETL(metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def _DATA_TYPE(self):
        pass

    @staticmethod
    def can_handle_data_type(self, data_type):
        return data_type == self._DATA_TYPE

    @staticmethod
    @abc.abstractmethod
    def run(self):
        pass

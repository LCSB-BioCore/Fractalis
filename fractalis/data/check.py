"""This module provides an abstract class for testing whether or not the
ETLs produce valid fractalis standard data."""

import abc
import logging

logger = logging.getLogger(__name__)


class IntegrityCheck(metaclass=abc.ABCMeta):
    """This is an abstract class that provides can be called directly"""

    @property
    @abc.abstractmethod
    def data_type(self) -> str:
        """Specifies a fractalis internal data type."""
        pass

    @classmethod
    def can_handle(cls, data_type: str) -> bool:
        """Test if this checker is responsible for the given data type.
        :param data_type: The data type that the ETL attempts to load.
        :return: True if this checker can handle the type.
        """
        return cls.data_type == data_type

    @staticmethod
    def factory(data_type: str) -> 'IntegrityCheck':
        """A factory that returns a checker object for a given data_type.
        :param data_type: Data type that one wants to test against.
        :return: An instance of IntegrityCheck
        """
        from . import CHECK_REGISTRY
        for Check in CHECK_REGISTRY:
            if Check.can_handle(data_type):
                return Check()
        error = "No IntegrityCheck implementation found " \
                "for data type '{}'".format(data_type)
        logger.error(error)
        raise NotImplementedError(error)

    @abc.abstractmethod
    def check(self, data: object) -> None:
        """Raise if the data have an invalid format. This is okay because
        there is no reason for the ETL to continue otherwise.
        :param data: The data to check.
        """
        pass

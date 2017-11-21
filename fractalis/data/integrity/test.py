"""This module tests nothing. It's just here for unit test mocking purposes."""

from fractalis.data.check import IntegrityCheck


class MockIntegrityCheck(IntegrityCheck):

    data_type = 'mock'

    def check(self, data: object) -> None:
        pass

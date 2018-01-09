"""This module provides PicSureHandler,
an implementation of ETLHandler for PIC-SURE."""

import logging

from fractalis.data.etlhandler import ETLHandler


logger = logging.getLogger(__name__)


class PicSureHandler(ETLHandler):
    """This ETLHandler provides integration with PIC-SURE."""

    _handler = 'pic-sure'

    @staticmethod
    def make_label(descriptor: dict) -> str:
        return descriptor['query']['select'][0]['alias']

    def _get_token_for_credentials(self, server: str, auth: dict) -> str:
        return auth['token']

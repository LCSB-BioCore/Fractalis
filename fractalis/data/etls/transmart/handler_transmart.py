"""This module provides TransmartHandler, an implementation of ETLHandler for
tranSMART."""

import logging

import requests

from fractalis.data.etlhandler import ETLHandler


logger = logging.getLogger(__name__)


class TransmartHandler(ETLHandler):
    """This ETLHandler provides integration with tranSMART.

    'tranSMART is a knowledge management platform that enables scientists to
    develop and refine research hypotheses by investigating correlations
    between genetic and phenotypic data, and assessing their analytical results
    in the context of published literature and other work.'

    Project URL: https://github.com/transmart
    """

    _handler = 'transmart'

    @staticmethod
    def make_label(descriptor: dict) -> str:
        return descriptor['label']

    def _get_token_for_credentials(self, server: str, auth: dict) -> str:
        try:
            user = auth['user']
            passwd = auth['passwd']
            if len(user) == 0 or len(passwd) == 0:
                raise KeyError
        except KeyError:
            error = "The authentication object must contain the non-empty " \
                    "fields 'user' and 'passwd'."
            logger.error(error)
            raise ValueError(error)
        r = requests.post(url=server + '/oauth/token',
                          params={
                              'grant_type': 'password',
                              'client_id': 'glowingbear-js',
                              'client_secret': '',
                              'username': user,
                              'password': passwd
                          },
                          headers={'Accept': 'application/json'},
                          timeout=10)
        if r.status_code != 200:
            error = "Could not authenticate. " \
                    "Reason: [{}]: {}".format(r.status_code, r.text)
            logger.error(error)
            raise ValueError(error)
        try:
            response = r.json()
            return response['access_token']
        except Exception:
            error = "Could not authenticate. " \
                    "Got unexpected response: '{}'".format(r.text)
            logger.error(error)
            raise ValueError(error)

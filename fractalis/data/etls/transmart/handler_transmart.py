"""This module provides TransmartHandler, an implementation of ETLHandler for
tranSMART."""

import requests

from fractalis.data.etlhandler import ETLHandler


class TransmartHandler(ETLHandler):
    """This ETLHandler provides integration with tranSMART.

    'tranSMART is a knowledge management platform that enables scientists to
    develop and refine research hypotheses by investigating correlations between
    genetic and phenotypic data, and assessing their analytical results in the
    context of published literature and other work.'

    Project URL: https://github.com/transmart
    """

    _handler = 'transmart'

    def _heartbeat(self):
        raise NotImplementedError()

    @staticmethod
    def make_label(descriptor: dict) -> str:
        return 'test'

    def _get_token_for_credentials(self, server: str,
                                   user: str, passwd: str) -> str:
        r = requests.get(url='{}/oauth/token?grant_type=password'
                             '&client_id=glowingbear-js'
                             '&client_secret='
                             '&username={}'
                             '&password={}'.format(server, user, passwd),
                         headers={'Accept': 'application/json'})
        if r.status_code != 200:
            raise ValueError("Could not authenticate. Reason: [{}]: {}"
                             .format(r.status_code, r.text))
        return r
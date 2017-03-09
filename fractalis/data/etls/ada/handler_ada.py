"""This module provides AdaHandler, an implementation of ETLHandler for ADA."""

import requests

from fractalis.data.etlhandler import ETLHandler


class AdaHandler(ETLHandler):
    """This ETLHandler provides integration with ADA.
    'Ada provides key infrastructure for secured integration, vizualization,
    and analysis of anonymized clinical and experimental data stored in CSV
    and tranSMART format, or provided by RedCAP and Synapse apps.'

    Project URL: https://git-r3lab.uni.lu/peter.banda/ncer-pd
    """

    _handler = 'ada'

    def _heartbeat(self):
        pass

    def _get_token_for_credentials(self, server: str,
                                   user: str, passwd: str) -> str:
        r = requests.post(url='{}/login'.format(server),
                          headers={'Accept': 'application/json'},
                          data={'id': user, 'password': passwd})
        if r.status_code != 200:
            raise ValueError("Could not authenticate. Reason: [{}]: {}"
                             .format(r.status_code, r.text))
        cookie = r.headers['Set-Cookie']
        token = [s for s in cookie.split(';')
                 if s.startswith('PLAY2AUTH_SESS_ID')][0]
        return token

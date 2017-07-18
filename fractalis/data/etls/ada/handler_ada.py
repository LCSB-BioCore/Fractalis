"""This module provides AdaHandler, an implementation of ETLHandler for ADA."""

import logging

import requests

from fractalis.data.etlhandler import ETLHandler


logger = logging.getLogger(__name__)


class AdaHandler(ETLHandler):
    """This ETLHandler provides integration with ADA.

    'Ada provides key infrastructure for secured integration, visualization,
    and analysis of anonymized clinical and experimental data stored in CSV
    and tranSMART format, or provided by RedCAP and Synapse apps.'

    Project URL: https://git-r3lab.uni.lu/peter.banda/ncer-pd
    """

    _handler = 'ada'

    def _heartbeat(self):
        raise NotImplementedError()

    @staticmethod
    def make_label(descriptor: dict) -> str:
        return '{} ({})'.format(descriptor['dictionary']['label'],
                                descriptor['data_set'])

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
        r = requests.post(url='{}/login'.format(server),
                          headers={'Accept': 'application/json'},
                          data={'id': user, 'password': passwd})
        if r.status_code != 200:
            raise ValueError("Could not authenticate. Reason: [{}]: {}"
                             .format(r.status_code, r.text))
        cookie = r.headers['Set-Cookie']
        token = [s for s in cookie.split(';')
                 if s.startswith('PLAY2AUTH_SESS_ID')][0]
        token = '='.join(token.split('=')[1:])  # remove PLAY2AUTH_SESS_ID=
        token = token[1:-1]  # remove surrounding ''
        return token

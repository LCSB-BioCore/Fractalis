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
            user = self.get_auth_value(auth, 'user')
            passwd = self.get_auth_value(auth, 'passwd')
            auth_service_type = self.get_auth_value(auth, 'authServiceType')
            if auth_service_type.upper() == 'OIDC':
                client_id = self.get_auth_value(auth, 'oidcClientId')
                url = self.get_auth_value(auth, 'oidcServerUrl')
            elif auth_service_type.upper() == 'TRANSMART':
                client_id = 'glowingbear-js'
                url = server + '/oauth/token'
            else:
                raise KeyError("The authentication service type in authentication object has to be one of "
                               "'oidc', 'transmart'")
        except KeyError as e:
            logger.error(e)
            raise ValueError(e)

        r = requests.post(url=url,
                          params={
                              'grant_type': 'password',
                              'client_id': client_id,
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

    @staticmethod
    def get_auth_value(auth: dict, property_name: str) -> str:
        value = auth[property_name]
        if len(value) == 0:
            raise KeyError(f'The authentication object must contain the non-empty field: "{property_name}"')
        return value

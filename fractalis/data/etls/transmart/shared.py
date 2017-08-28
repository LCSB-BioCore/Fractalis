"""This module provides shared functionality to the transmart ETLs."""

import logging

import requests


logger = logging.getLogger(__name__)


def extract_data(server: str, descriptor: dict, token: str) -> dict:
    """Extract data from transmart.
    :param server: The target server host.
    :param descriptor: Dict describing the data to download.
    :param token: The token used for authentication.
    """
    r = requests.get(url='{}/v2/observations'.format(server),
                     params={
                         'constraint': '{{"type": "concept","path": "{}"}}'
                                       ''.format(descriptor["path"]),
                         'type': 'autodetect'
                     },
                     headers={
                         'Accept': 'application/json',
                         'Authorization': 'Bearer {}'.format(token)
                     },
                     timeout=2000)
    if r.status_code != 200:
        error = "Target server responded with " \
                "status code {}.".format(r.status_code)
        logger.error(error)
        raise ValueError(error)
    try:
        return r.json()
    except Exception as e:
        logger.exception(e)
        raise ValueError("Got unexpected data format.")

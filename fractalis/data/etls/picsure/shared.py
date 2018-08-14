from time import sleep
import logging

import requests

from fractalis import app


logger = logging.getLogger(__name__)


def raise_for_status(r):
    if r.status_code != requests.codes.ok:
        error = f'PIC-SURE API reported an error: [{r.status_code}] {r.text}'
        logger.exception(error)
        raise RuntimeError(error)


def submit_query(query: str, server: str, token: str) -> int:
    r = requests.post(
        url='{}/queryService/runQuery'.format(server),
        data=query,
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(token)
        },
        verify=app.config['ETL_VERIFY_SSL_CERT']
    )
    raise_for_status(r)
    result_id = r.json()['resultId']
    return result_id


def wait_for_completion(result_id: int, server, token):
    def _check_status():
        r = requests.get(
            url='{}/resultService/resultStatus/{}'.format(
                server, result_id),
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {}'.format(token)
            },
            verify=app.config['ETL_VERIFY_SSL_CERT']
        )
        raise_for_status(r)
        # noinspection PyBroadException
        try:
            return r.json()['status']
        except Exception:
            return r.text
    while True:
        status = _check_status()
        if status == 'RUNNING':
            sleep(1)
            continue
        elif status == 'AVAILABLE':
            break
        else:
            error = f'PIC-SURE API reported an error: {status}'
            logger.exception(error)
            raise RuntimeError(error)


def get_data(result_id, server, token):
    r = requests.get(
        url='{}/resultService/result/{}/CSV'.format(
            server, result_id),
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(token)
        },
        verify=app.config['ETL_VERIFY_SSL_CERT']
    )
    raise_for_status(r)
    if not r.text:
        error = f'PIC-SURE API returned no data.'
        logger.exception(error)
        raise RuntimeError(error)
    return r.text

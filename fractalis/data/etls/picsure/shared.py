from time import sleep

import requests

from fractalis import app


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
    r.raise_for_status()
    result_id = r.json()['resultId']
    return result_id


def wait_for_completion(result_id: int, server, token):
    def _check_status():
        return requests.get(
            url='{}/resultService/resultStatus/{}'.format(
                server, result_id),
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {}'.format(token)
            },
            verify=app.config['ETL_VERIFY_SSL_CERT']
        ).json()
    while _check_status()['status'] == 'RUNNING':
        sleep(1)


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
    r.raise_for_status()
    return r.text

from time import sleep

import requests


def submit_query(query: str, server: str, token: str) -> int:
    r = requests.post(
        url='{}/rest/v1/queryService/runQuery'.format(server),
        data=query,
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(token)
        }
    )
    r.raise_for_status()
    result_id = r.json()['resultId']
    return result_id


def wait_for_completion(result_id: int, server, token):
    def _check_status():
        return requests.get(
            url='{}/rest/v1/resultService/resultStatus/{}'.format(
                server, result_id),
            headers={
                'Content-Type': 'application/json',
                'Authorization': 'Bearer {}'.format(token)
            }
        ).json()
    while _check_status()['status'] == 'RUNNING':
        sleep(1)


def get_data(result_id, server, token):
    r = requests.get(
        url='{}/rest/v1/resultService/result/{}/CSV'.format(
            server, result_id),
        headers={
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {}'.format(token)
        }
    )
    r.raise_for_status()
    return r.text

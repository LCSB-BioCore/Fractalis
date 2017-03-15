"""This module contains code that is shared between the different ETLs."""

from typing import List

import requests


def make_cookie(token: str) -> dict:
    split = token.split('=')
    assert len(split) == 2
    cookie = {split[0]: split[1][1:-1]}
    return cookie


def get_field(server: str, data_set: str,
            cookie: dict, projections: List[str]) -> dict:
    r = requests.get(url='{}/studies/records/findCustom'.format(server),
                     headers={'Accept': 'application/json'},
                     params={
                         'dataSet': data_set,
                         'projection': projections
                     },
                     cookies=cookie)
    if r.status_code != 200:
        raise ValueError("Data extraction failed. Reason: [{}]: {}"
                         .format(r.status_code, r.text))
    return r.json()


def get_dictionary(server: str, data_set: str,
                   descriptor: dict, cookie: dict) -> dict:
    r = requests.get(url='{}/studies/dictionary/get/{}'
                     .format(server, descriptor['projection']),
                     headers={'Accept': 'application/json'},
                     params={'dataSet': data_set},
                     cookies=cookie)
    if r.status_code != 200:
        dictionary = None
        # TODO: Log this
        pass
    else:
        dictionary = r.json()
    return dictionary


def prepare_ids(data: List[dict]) -> List[dict]:
    new_data = []
    for row in data:
        id = row['_id']['$oid']
        del row['_id']
        row['id'] = id
        new_data.append(row)
    return new_data


def name_to_label(data: List[dict], dictionary: dict) -> List[dict]:
    try:
        label = dictionary['label']
    except (KeyError, TypeError):
        return data
    for row in data:
        if dictionary['name'] in row:
            value = row[dictionary['name']]
            del row[dictionary['name']]
            row[label] = value
    return data
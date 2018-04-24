"""This module contains code that is shared between the different ETLs."""

import logging
from typing import List

import pandas as pd
import requests


logger = logging.getLogger(__name__)


def make_cookie(token: str) -> dict:
    return {'PLAY2AUTH_SESS_ID': token}


def get_field(server: str, data_set: str,
              cookie: dict, projection: str) -> List[dict]:
    r = requests.get(url='{}/dataSets/records/findCustom'.format(server),
                     headers={'Accept': 'application/json'},
                     params={
                         'dataSet': data_set,
                         'projection': ['_id', projection],
                         'filterOrId': '[{{"fieldName":"{}","conditionType":"!=","value":""}}]'.format(projection)  # noqa: 501
                     },
                     cookies=cookie,
                     timeout=60)
    if r.status_code != 200:
        error = "Target server responded with " \
                "status code {}.".format(r.status_code)
        logger.error(error)
        raise ValueError(error)
    try:
        field_data = r.json()
    except Exception as e:
        logger.exception(e)
        raise TypeError("Unexpected data format. "
                        "Possible authentication error.")
    return field_data


def prepare_ids(data: List[dict]) -> List[dict]:
    new_data = []
    for row in data:
        id = row['_id']['$oid']
        del row['_id']
        row['id'] = id
        new_data.append(row)
    return new_data


def name_to_label(data: List[dict], descriptor: dict) -> List[dict]:
    try:
        label = descriptor['dictionary']['label']
    except (KeyError, TypeError):
        return data
    for row in data:
        if descriptor['dictionary']['name'] in row:
            value = row[descriptor['dictionary']['name']]
            del row[descriptor['dictionary']['name']]
            row[label] = value
    return data


def make_data_frame(data: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df = pd.melt(df, id_vars='id', var_name='feature')
    return df

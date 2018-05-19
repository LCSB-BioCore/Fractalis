"""This module provides shared functionality to the transmart ETLs."""

import logging
import pandas as pd
from urllib.parse import unquote_plus

import requests

from fractalis.data.etl import ETL

logger = logging.getLogger(__name__)

NUMERICAL_FIELD = 'numericValue'
CATEGORICAL_FIELD = 'stringValue'


def extract_data(server: str, descriptor: dict, token: str) -> dict:
    """Extract data from transmart.
    :param server: The target server host.
    :param descriptor: Dict describing the data to download.
    :param token: The token used for authentication.
    """
    params = dict(
        constraint=descriptor['constraint'],
        type='clinical'
    )
    if descriptor['data_type'] == 'numerical_array':
        params['type'] = 'autodetect'
        params['projection'] = 'log_intensity'

        if 'biomarker_constraint' in descriptor:
            params['biomarker_constraint'] = descriptor['biomarker_constraint']

    r = requests.get(url='{}/v2/observations'.format(server),
                     params=params,
                     headers={
                         'Accept': 'application/json',
                         'Authorization': 'Bearer {}'.format(token)
                     },
                     timeout=2000)

    logger.info('URL called: {}'.format(
        unquote_plus(r.url))
    )
    if r.status_code != 200:
        error = "Target server responded with status code {}. Message: {}.".\
            format(r.status_code, r.json())
        logger.error(error)
        raise ValueError(error)

    try:
        return r.json()
    except Exception as e:
        logger.exception(e)
        raise ValueError("Got unexpected data format.")


def get_dimension_index(obs, dimension):
    return list(obs['dimensionElements'].keys()).index(dimension)


def get_dimension_element(obs, dimension, index):
    return obs['dimensionElements'][dimension][index]


def transform_clinical(raw_data: dict, value_field: str) -> pd.DataFrame:
    patient_idx = get_dimension_index(raw_data, 'patient')
    rows = []

    for entry in raw_data['cells']:
        patient_element = entry['dimensionIndexes'][patient_idx]
        patient = get_dimension_element(raw_data, 'patient', patient_element)

        rows.append([
            patient['inTrialId'],
            entry[value_field]
        ])

    df = pd.DataFrame(rows, columns=['id', 'value'])
    feature = df.columns[1]
    df.insert(1, 'feature', feature)
    return df


def transform_highdim(raw_data: dict):
    sample_idx = get_dimension_index(raw_data, 'assay')
    feature_idx = get_dimension_index(raw_data, 'biomarker')
    rows = []

    for entry in raw_data['cells']:
        sample_element = entry['dimensionIndexes'][sample_idx]
        sample = get_dimension_element(raw_data, 'assay', sample_element)

        feature_element = entry['dimensionIndexes'][feature_idx]
        feature = get_dimension_element(raw_data, 'biomarker', feature_element)

        rows.append([
            sample['sampleCode'],
            entry[NUMERICAL_FIELD],
            feature['label']
        ])

    df = pd.DataFrame(rows, columns=['id', 'value', 'feature'])
    return df


def create_etl_type(name_, produces_, field_name):
    """
    Create a ETL task class based on a specific input type.

    :param name_: task name for registry.
    :param produces_: output type.
    :param field_name: name of cell in observation (numericValue, stringValue)
    :return: ETL task class
    """
    class TransmartETL(ETL):

        name = name_
        produces = produces_

        @staticmethod
        def can_handle(handler: str, descriptor: dict) -> bool:
            return handler == 'transmart' and descriptor['data_type'] == produces_

        def extract(self, server: str, token: str, descriptor: dict) -> dict:
            return extract_data(server=server, descriptor=descriptor, token=token)

        def transform(self, raw_data: dict, descriptor: dict) -> pd.DataFrame:
            if self.produces in ('numerical', 'categorical'):
                return transform_clinical(raw_data, field_name)
            if self.produces == 'numerical_array':
                return transform_highdim(raw_data)

    return TransmartETL

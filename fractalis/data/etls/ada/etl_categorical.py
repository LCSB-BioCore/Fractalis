"""Default ETL to get data from ADA"""

import requests
from pandas import DataFrame

from fractalis.data.etl import ETL


class CategoricalETL(ETL):
    """The default ETL for ADA."""

    name = 'ada_categorical_etl'
    _handler = 'ada'
    _accepts = ['default', 'Enum']
    produces = 'categorical'

    def extract(self, server: str,
                token: str, descriptor: dict) -> object:
        data_set = descriptor['data_set']
        projections = ['_id']  # we always ask for an id
        projections += [descriptor['projection']]
        split = token.split('=')
        assert len(split) == 2
        cookie = {split[0]: split[1][1:-1]}
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
        data = r.json()
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

        return {'data': data, 'dictionary': dictionary}

    def transform(self, raw_data) -> DataFrame:
        data = raw_data['data']
        dictionary = raw_data['dictionary']

        new_data = []
        for row in data:
            id = row['_id']['$oid']
            del row['_id']
            row['id'] = id
            new_data.append(row)

        if dictionary is not None:
            for row in new_data:
                assert len(row) == 2
                value = row[dictionary['name']]
                if dictionary['fieldType'] == 'Enum':
                    value = dictionary['numValues'][str(value)]
                del row[dictionary['name']]
                row[dictionary['label']] = value

        data_frame = DataFrame(new_data)
        return data_frame

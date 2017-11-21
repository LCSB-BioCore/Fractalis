"""This module provides the integrity checks
for the 'numerical_array' fractalis format."""

import logging

import numpy as np
import pandas as pd

from fractalis.data.check import IntegrityCheck

logger = logging.getLogger(__name__)


class NumericalArrayIntegrityCheck(IntegrityCheck):
    """Implements IntegrityCheck for 'numerical_array' data type."""

    data_type = 'numerical_array'

    def check(self, data: object) -> None:
        if not isinstance(data, pd.DataFrame):
            error = "Data must be a pandas.DataFrame."
            logger.error(error)
            raise ValueError(error)
        if sorted(['id', 'feature', 'value']) != sorted(data.columns.tolist()):
            error = "Data frame must contain the columns " \
                    "'id', 'feature', and 'value'."
            logger.error(error)
            raise ValueError(error)
        if data['id'].dtype != np.object:
            error = "'id' column must be of type 'object' ('string')."
            logger.error(error)
            raise ValueError(error)
        if data['feature'].dtype != np.object:
            error = "'feature' column must be of type 'object' ('string')."
            logger.error(error)
            raise ValueError(error)
        if data['value'].dtype != np.int \
                and data['value'].dtype != np.float:
            error = "'value' column must be of type 'np.int' or 'np.float'."
            logger.error(error)
            raise ValueError(error)
        if data.groupby(['id', 'feature']).count().shape[0] != data.shape[0]:
            error = "Every combination of 'id' and 'feature' must be unique."
            logger.error(error)
            raise ValueError(error)

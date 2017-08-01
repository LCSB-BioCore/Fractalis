"""This module contains common array functionality used in analytic tasks."""

import logging
from copy import deepcopy

from typing import List, TypeVar

import pandas as pd


logger = logging.getLogger(__name__)

T = TypeVar('T')

_protected_colnames = ['variable']


def drop_ungrouped_samples(df: pd.DataFrame,
                           subsets: List[List[T]]) -> pd.DataFrame:
    """Drop samples cols that are no present in any of the subsets.
    :param df: Unmodified data frame
    submitted to the main method of an AnalyticTask.
    :param subsets: Subgroups defined by the user.
    :return: Filtered data frame.
    """
    flattened_subsets = [x for subset in subsets for x in subset]
    if not flattened_subsets:
        error = "Subsets must not be empty."
        logger.error(error)
        raise ValueError(error)
    colnames = list(set(flattened_subsets))
    colnames += _protected_colnames # special colnames that we want to keep
    colnames = [colname for colname in list(df) if colname in colnames]
    df = df[colnames]
    return df


def drop_unused_subset_ids(df: pd.DataFrame,
                           subsets: List[List[T]]) -> List[List[T]]:
    """Drop subset ids that are not present in the given data
    :param df: Unmodified data frame
    submitted to the main method of an AnalyticTask.
    :param subsets: Subset groups specified by the user.
    :return: Modified subsets list.
    """
    df_ids = list(df)
    df_ids = [el for el in df_ids if el not in _protected_colnames]
    _subsets = deepcopy(subsets)
    for subset in _subsets:
        _subset = list(subset)
        for id in _subset:
            if id not in df_ids:
                subset.remove(id)
    return _subsets

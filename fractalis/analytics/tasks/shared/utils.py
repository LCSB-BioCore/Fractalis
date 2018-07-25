"""This module contains common functions used in analytic tasks."""

import logging
from typing import List, TypeVar
from functools import reduce
from copy import deepcopy

import pandas as pd
import numpy as np


logger = logging.getLogger(__name__)


def apply_subsets(df: pd.DataFrame,
                  subsets: List[List[str]]) -> pd.DataFrame:
    """Build a new DataFrame that contains a new column 'subset' defining
    the subset the data point belongs to. If a data point belongs to
    multiple subsets then the row is duplicated.
    :param df: The DataFrame used as a base.
    :param subsets: The subsets defined by the user.
    :return: The new DataFrame with an additional 'subset' column.
    """
    if not subsets:
        subsets = [df['id']]
    _df = pd.DataFrame()
    for i, subset in enumerate(subsets):
        df_subset = df[df['id'].isin(subset)]
        if not df_subset.shape[0]:
            continue
        subset_col = [i] * df_subset.shape[0]
        df_subset = df_subset.assign(subset=subset_col)
        _df = _df.append(df_subset)
    if _df.shape[0] == 0:
        raise ValueError("No data match given subsets.")
    return _df


def apply_categories(df: pd.DataFrame,
                     categories: List[pd.DataFrame]) -> pd.DataFrame:
    """Collapse all category DataFrames into a single column and add it as a new
     column to the given DataFrame. This is done by joining all columns values
    via the string `&&`
    :param df: The DataFrame to be extended
    :param categories: List of category DataFrames
    :return: The base DataFrame with an additional 'category' column
    """
    if len(categories):
        # drop 'feature' column from dfs
        categories = [df.drop('feature', axis=1) for df in categories]
        # merge all dfs into one
        data = reduce(lambda l, r: l.merge(r, on='id', how='outer'),
                      categories)
        # remember ids
        ids = data['id']
        # drop id column
        data = data.drop('id', axis=1)
        # replace everything that is not an category with ''
        data = data.applymap(
            lambda el: el if isinstance(el, str) and el else '')
        # join all columns with && into a single one. Ignore '' entries.
        data = data.apply(
            lambda row: ' AND '.join(
                list(map(str, [el for el in row.tolist() if el]))), axis=1)
        # cast Series to DataFrame
        data = pd.DataFrame(data, columns=['category'])
        # reassign ids to collapsed df
        data = data.assign(id=ids)
        # merge category data into main df
        df = df.merge(data, on='id', how='left')
        # get unique categories
    else:
        df = df.assign(category='')
    return df


def apply_id_filter(df: pd.DataFrame, id_filter: List[str]) -> pd.DataFrame:
    """Keep only rows where id is in id_filter. If id_filter is empty keep all.
    :param df: Dataframe containing array data in the Fractalis format.
    :param id_filter: List of ids to keep.
    """
    if id_filter:
        df = df[df['id'].isin(id_filter)]
    return df


def drop_unused_subset_ids(df: pd.DataFrame,
                           subsets: List[List[str]]) -> List[List[str]]:
    """Drop subset ids that are not present in the given data
    :param df: Dataframe containing array data in the Fractalis format.
    :param subsets: Subset groups specified by the user.
    :return: Modified subsets list.
    """
    ids = df['id'].unique()
    _subsets = deepcopy(subsets)
    for subset in _subsets:
        _subset = list(subset)
        for id in _subset:
            if id not in ids:
                subset.remove(id)
    return _subsets


def apply_transformation(df: pd.DataFrame, transformation: str) -> pd.DataFrame:
    """Apply transformation to the value column of the data frame.
    E.g. log2 or 10^x scales.
    NaN and Inf are dropped!
    :param df: Dataframe containing array data in the Fractalis format.
    :param transformation: The transformation to apply.
    :return: The dataframe with an transformed value column excl. NaN and Inf
    """
    transformations = {
        'identity': lambda x: x,
        'log2(x)': np.log2,
        'log10(x)': np.log10,
        '2^x': lambda x: np.power(2.0, x),
        '10^x': lambda x: np.power(10.0, x)
    }
    # drop zeros because
    df = df[df['value'] != 0]
    df['value'] = transformations[transformation](df['value'])
    if np.any(np.isinf(df['value'])):
        error = 'Found inf after transformation. Transformation "{}" should ' \
                'only be used on log scaled data.'.format(transformation)
        logger.exception(error)
        raise ValueError(error)
    return df

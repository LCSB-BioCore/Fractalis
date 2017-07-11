"""This module contains common functions used in analytic tasks."""

from typing import List, TypeVar
from functools import reduce

import pandas as pd


T = TypeVar('T')


def apply_subsets(df: pd.DataFrame,
                  subsets: List[List[T]]) -> pd.DataFrame:
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
        raise ValueError("No data match given subsets. Keep in mind that X "
                         "and Y are intersected before the subsets are "
                         "applied.")
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
        # merge all dfs into one
        data = reduce(lambda l, r: l.merge(r, on='id', how='outer'), categories)
        # remember ids
        ids = data['id']
        # drop id column
        data = data.drop('id', axis=1)
        # replace everything that is not an category with ''
        data = data.applymap(lambda el: el if isinstance(el, str) and el else '')
        # join all columns with && into a single one. Ignore '' entries.
        data = data.apply(lambda row: ' AND '.join(list(map(str, [el for el in row.tolist() if el]))), axis=1)
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


def apply_id_filter(df: pd.DataFrame, id_filter: list) -> pd.DataFrame:
    """Throw away all rows whose id is not in id_filter.
    :param df: The DataFrame to filter.
    :param id_filter: The filter.
    :return: The filtered DataFrame.
    """
    if id_filter:
        df = df[df['id'].isin(id_filter)]
    if df.shape[0] == 0:
        raise ValueError("The current selection does not match any data.")
    return df

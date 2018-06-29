"""This module provides statistics for mRNA and miRNA data."""

from copy import deepcopy
from typing import List, TypeVar
from collections import OrderedDict
import logging

import pandas as pd
import numpy as np
from rpy2 import robjects as robj
from rpy2.robjects import r, pandas2ri
from rpy2.robjects.packages import importr


T = TypeVar('T')
importr('limma')
importr('DESeq2')
pandas2ri.activate()
logger = logging.getLogger(__name__)


def get_stats(df: pd.DataFrame, subsets: List[List[T]],
              params: dict, ranking_method: str) -> pd.DataFrame:
    if ranking_method == 'mean':
        stats = get_mean_stats(df)
    elif ranking_method == 'median':
        stats = get_median_stats(df)
    elif ranking_method == 'variance':
        stats = get_variance_stats(df)
    elif ranking_method == 'limma':
        stats = get_limma_stats(df, subsets)
    elif ranking_method == 'DESeq2':
        stats = get_deseq2_stats(df, subsets, **params)
    else:
        error = "Static method unknown: {}".format(ranking_method)
        logger.exception(error)
        raise NotImplementedError(error)
    return stats


def get_mean_stats(df: pd.DataFrame) -> pd.DataFrame:
    means = np.mean(df, axis=1)
    stats = pd.DataFrame(means, columns=['mean'])
    stats['feature'] = df.index
    return stats


def get_median_stats(df: pd.DataFrame) -> pd.DataFrame:
    medians = np.median(df, axis=1)
    stats = pd.DataFrame(medians, columns=['median'])
    stats['feature'] = df.index
    return stats


def get_variance_stats(df: pd.DataFrame) -> pd.DataFrame:
    variances = np.var(df, axis=1)
    stats = pd.DataFrame(variances, columns=['variance'])
    stats['feature'] = df.index
    return stats


def get_limma_stats(df: pd.DataFrame, subsets: List[List[T]]) -> pd.DataFrame:
    """Use the R bioconductor package 'limma' to perform a differential
    gene expression analysis on the given data frame.
    :param df: Matrix of measurements where each column represents a sample
    and each row a gene/probe.
    :param subsets: Groups to compare with each other.
    :return: Results of limma analysis. More than 2 subsets will result in
    a different structured result data frame. See ?topTableF in R.
    """
    logger.debug("Computing limma stats")
    # prepare the df in case an id exists in more than one subset
    if len(subsets) < 2:
        error = "Limma analysis requires at least " \
                "two non-empty groups for comparison."
        logger.error(error)
        raise ValueError(error)
    if df.shape[0] < 1 or df.shape[1] < 2:
        error = "Limma analysis requires a " \
                "data frame with dimension 1x2 or more."
        logger.error(error)
        raise ValueError(error)

    flattened_subsets = [x for subset in subsets for x in subset]
    df = df[flattened_subsets]
    ids = list(df)
    features = df.index

    # creating the design vector according to the subsets
    design_vector = [''] * len(ids)
    subsets_copy = deepcopy(subsets)
    for i, sample_id in enumerate(ids):
        for j, subset in enumerate(subsets_copy):
            try:
                subset.index(sample_id)  # raises an Exception if not found
                subset.remove(sample_id)
                design_vector[i] = str(j + 1)
                break
            except ValueError:
                assert j != len(subsets_copy) - 1

    assert '' not in design_vector

    # create group names
    groups = ['group{}'.format(i + 1) for i in list(range(len(subsets)))]

    # create a string for each pairwise comparison of the groups
    comparisons = []
    for i in reversed(range(len(subsets))):
        for j in range(i):
            comparisons.append('group{}-group{}'.format(i+1, j+1))

    # fitting according to limma doc Chapter 8: Linear Models Overview
    r_form = robj.Formula('~ 0+factor(c({}))'.format(','.join(design_vector)))
    r_design = r['model.matrix'](r_form)
    r_design.colnames = robj.StrVector(groups)
    r_data = pandas2ri.py2ri(df)
    # py2ri is stupid and makes too many assumptions.
    # These two lines restore the column order
    r_data.colnames = list(OrderedDict.fromkeys(ids))
    r_data = r_data.rx(robj.StrVector(ids))

    r_fit = r['lmFit'](r_data, r_design)
    r_contrast_matrix = r['makeContrasts'](*comparisons, levels=r_design)
    r_fit_2 = r['contrasts.fit'](r_fit, r_contrast_matrix)
    r_fit_2 = r['eBayes'](r_fit_2)
    r_results = r['topTable'](r_fit_2, number=float('inf'),
                              sort='none', genelist=features)
    results = pandas2ri.ri2py(r_results)
    # let's give the gene list column an appropriate name
    colnames = results.columns.values
    colnames[0] = 'feature'
    results.columns = colnames

    return results


# FIXME: Add more parameters (e.g. for filtering low count rows)
def get_deseq2_stats(df: pd.DataFrame,
                     subsets: List[List[T]],
                     min_total_row_count: int = 0) -> pd.DataFrame:
    """Use the R bioconductor package 'limma' to perform a differential
    expression analysis of count like data (e.g. miRNA). See package
    documentation for more details.
    :param df: Matrix of counts, where each column is a sample and each row
    a feature.
    :param subsets: The two subsets to compare with each other.
    :param min_total_row_count: Drop rows that have in total less than than
        min_total_row_count reads
    :return: Results of the analysis in form of a Dataframe (p, logFC, ...)
    """
    logger.debug("Computing deseq2 stats")
    if len(subsets) != 2:
        error = "This method currently only supports exactly two " \
                "subsets as this is the most common use case. Support " \
                "for more subsets will be added later."
        logger.exception(error)
        raise ValueError(error)
    # flatten subset
    flattened_subsets = [x for subset in subsets for x in subset]
    # discard columns that are not in a subset
    df = df[flattened_subsets]
    # filter rows with too few reads
    total_row_counts = df.sum(axis=1)
    keep = total_row_counts[total_row_counts >= min_total_row_count].index
    df = df.loc[keep]
    # pandas df -> R df
    r_count_data = pandas2ri.py2ri(df)
    # py2ri is stupid and makes too many assumptions.
    # These two lines restore the column order
    r_count_data.colnames = list(OrderedDict.fromkeys(flattened_subsets))
    r_count_data = r_count_data.rx(robj.StrVector(flattened_subsets))

    # see package documentation
    condition = ['s{}'.format(i) for i, subset in enumerate(subsets)
                 for _ in subset]
    r_condition = robj.FactorVector(robj.StrVector(condition))
    r_col_data = r['DataFrame'](condition=r_condition)
    r_design = robj.Formula('~ condition')
    r_design.environment['condition'] = r_condition
    r_dds = r['DESeqDataSetFromMatrix'](r_count_data, r_col_data, r_design)
    r_dds = r['DESeq'](r_dds, parallel=True)
    r_res = r['results'](r_dds)

    # R result table to Python pandas
    r_res = r['as.data.frame'](r_res)
    results = pandas2ri.ri2py(r_res)
    results.insert(0, 'feature', list(r['row.names'](r_res)))
    return results

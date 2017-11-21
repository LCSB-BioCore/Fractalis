"""This module provides row-wise statistics for the heat map."""

from copy import deepcopy
from typing import List, TypeVar
import logging

import pandas as pd
from rpy2 import robjects as R
from rpy2.robjects import r, pandas2ri
from rpy2.robjects.packages import importr

from fractalis.analytics.task import AnalyticTask


T = TypeVar('T')
importr('limma')
pandas2ri.activate()
logger = logging.getLogger(__name__)


class StatisticTask(AnalyticTask):

    name = 'expression-stats-task'

    def main(self, df: pd.DataFrame, subsets: List[List[T]],
             ranking_method: str) -> pd.DataFrame:
        if ranking_method == 'mean':
            stats = self.get_mean_stats(df)
        elif ranking_method == 'median':
            stats = self.get_median_stats(df)
        elif ranking_method == 'variance':
            stats = self.get_variance_stats(df)
        else:
            stats = self.get_limma_stats(df, subsets)
        return stats

    @staticmethod
    def get_mean_stats(df: pd.DataFrame) -> pd.DataFrame:
        means = [row.mean() for row in df.values]
        stats = pd.DataFrame(means, columns=['mean'])
        stats['feature'] = df.index
        return stats

    @staticmethod
    def get_median_stats(df: pd.DataFrame) -> pd.DataFrame:
        means = [row.median() for row in df.values]
        stats = pd.DataFrame(means, columns=['median'])
        stats['feature'] = df.index
        return stats

    @staticmethod
    def get_variance_stats(df: pd.DataFrame) -> pd.DataFrame:
        means = [row.var() for row in df.values]
        stats = pd.DataFrame(means, columns=['var'])
        stats['feature'] = df.index
        return stats

    @staticmethod
    def get_limma_stats(df: pd.DataFrame,
                        subsets: List[List[T]]) -> pd.DataFrame:
        """Use the R bioconductor package 'limma' to perform a differential
        gene expression analysis on the given data frame.
        :param df: Matrix of measurements where each column represents a sample
        and each row a gene/probe.
        :param subsets: Groups to compare with each other.
        :return: Results of limma analysis. More than 2 subsets will result in
        a different structured result data frame. See ?topTableF in R.
        """
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
        for i, id in enumerate(ids):
            for j, subset in enumerate(subsets_copy):
                try:
                    subset.index(id)  # raises an Exception if not found
                    subset.remove(id)
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
        r_form = R.Formula('~ 0+factor(c({}))'.format(','.join(design_vector)))
        r_design = r['model.matrix'](r_form)
        r_design.colnames = R.StrVector(groups)
        r_data = pandas2ri.py2ri(df)
        # the next two lines are necessary if column ids are not unique,
        # because the python to r transformation drops those columns otherwise
        r_ids = R.StrVector(['X{}'.format(id) for id in ids])
        r_data = r_data.rx(r_ids)
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

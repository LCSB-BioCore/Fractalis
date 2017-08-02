"""Module containing analysis code for heatmap analytics."""

from copy import deepcopy
from typing import List, TypeVar
from functools import reduce
import logging

import pandas as pd
from scipy.stats import zscore
from rpy2 import robjects as R
from rpy2.robjects import r, pandas2ri
from rpy2.robjects.packages import importr

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared.array_utils \
    import drop_ungrouped_samples, drop_unused_subset_ids


importr('limma')
pandas2ri.activate()

T = TypeVar('T')
logger = logging.getLogger(__name__)


class HeatmapTask(AnalyticTask):
    """Heatmap Analysis Task implementing AnalyticsTask. This class is a
    submittable celery task."""

    name = 'compute-heatmap'

    def main(self, numerical_arrays: List[pd.DataFrame],
             numericals: List[pd.DataFrame],
             categoricals: List[pd.DataFrame],
             subsets: List[List[T]]) -> dict:
        # prepare inputs args
        df = reduce(lambda a, b: a.append(b), numerical_arrays)
        if not subsets:
            ids = list(df)
            ids.remove('variable')
            subsets = [ids]
        df = drop_ungrouped_samples(df=df, subsets=subsets)
        subsets = drop_unused_subset_ids(df=df, subsets=subsets)

        # make sure the input data are still valid after the pre-processing
        if df.shape[0] < 1 or df.shape[1] < 2:
            error = "Either the input data set is too small or " \
                    "the subset sample ids do not match the data."
            logger.error(error)
            raise ValueError(error)

        for subset in subsets:
            if not subset:
                error = "One or more of the specified subsets does not " \
                        "match any sample id for the given array data."
                logger.error(error)
                raise ValueError(error)

        # create z-score matrix used for visualising the heatmap
        variables = df['variable']
        zscores = df.drop('variable', axis=1)
        zscores = zscores.apply(zscore, axis=1)
        zscores.insert(0, 'variable', variables)

        # execute differential gene expression analysis
        stats = self.get_limma_stats(df, subsets)

        # prepare output for front-end
        df = self.melt_standard_format_df(df)
        zscores = self.melt_standard_format_df(zscores)
        df = pd.merge(df, zscores, on=['id', 'variable'])
        df.columns = ['id', 'variable', 'value', 'zscore']

        return {
            'data': df.to_json(orient='index'),
            'stats': stats.to_json(orient='index')
        }

    def melt_standard_format_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.shape[0] < 1 or df.shape[1] < 2:
            error = "Data must be non-empty for melting."
            logger.error(error)
            raise ValueError(error)
        variables = df['variable']
        df.drop('variable', axis=1, inplace=True)
        df = df.T
        df.columns = variables
        df.index.name = 'id'
        df.reset_index(inplace=True)
        df = pd.melt(df, id_vars='id')
        return df

    def get_limma_stats(self, df: pd.DataFrame,
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
                    "two groups for comparison."
            logger.error(error)
            raise ValueError(error)
        if df.shape[0] < 1 or df.shape[1] < 2:
            error = "Limma analysis requires a " \
                    "data frame with dimension 1x2 or more."
            logger.error(error)
            raise ValueError(error)

        # for analysis we want only sample cols
        variables = df['variable']
        df = df.drop('variable', axis=1)

        flattened_subsets = [x for subset in subsets for x in subset]
        df = df[flattened_subsets]
        ids = list(df)
        logger.critical(ids)

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
        # the next two lines are necessary if column ids are not unique, because
        # the python to r transformation drops those columns otherwise
        r_ids = R.StrVector(['X{}'.format(id) for id in ids])
        r_data = r_data.rx(r_ids)
        r_fit = r['lmFit'](r_data, r_design)
        r_contrast_matrix = r['makeContrasts'](*comparisons, levels=r_design)
        r_fit_2 = r['contrasts.fit'](r_fit, r_contrast_matrix)
        r_fit_2 = r['eBayes'](r_fit_2)
        r_results = r['topTable'](r_fit_2, number=float('inf'),
                                  sort='none', genelist=variables)
        results = pandas2ri.ri2py(r_results)
        # let's give the gene list column an appropriate name
        colnames = results.columns.values
        colnames[0] = 'variable'
        results.columns = colnames

        return results

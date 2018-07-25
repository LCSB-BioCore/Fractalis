"""This module provides statistics for a Survival Analysis."""

import logging
from typing import List

import pandas as pd
import numpy as np
from lifelines import KaplanMeierFitter, NelsonAalenFitter

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared import utils


logger = logging.getLogger(__name__)


class SurvivalTask(AnalyticTask):
    """Survival Analysis Task implementing AnalyticTask.
    This class is a submittable celery task."""

    name = 'survival-analysis'

    def main(self, durations: List[pd.DataFrame],
             categories: List[pd.DataFrame],
             event_observed: List[pd.DataFrame],
             estimator: str,
             id_filter: List[str],
             subsets: List[List[str]]) -> dict:
        # TODO: Docstring
        if len(durations) != 1:
            error = 'Analysis requires exactly one array that specifies the ' \
                    'duration length.'
            logger.exception(error)
            raise ValueError(error)
        if len(event_observed) > 1:
            error = 'Maximal one variable for "event_observed" allowed'
            logger.exception(error)
            raise ValueError(error)

        df = durations[0]
        df.dropna(inplace=True)
        df = utils.apply_id_filter(df=df, id_filter=id_filter)
        df = utils.apply_subsets(df=df, subsets=subsets)
        df = utils.apply_categories(df=df, categories=categories)

        stats = {}
        categories = df['category'].unique().tolist()
        subsets = df['subset'].unique().tolist()
        # for every category and subset combination estimate the survival fun.
        for category in categories:
            for subset in subsets:
                sub_df = df[(df['category'] == category) &
                            (df['subset'] == subset)]
                T = sub_df['value']
                E = None  # default is nothing is censored
                if len(T) <= 3:
                    continue
                if event_observed:
                    # find observation boolean value for every duration
                    E = event_observed[0].merge(sub_df, how='right', on='id')
                    E = [bool(x) and not np.isnan(x) for x in E['value']]
                    assert len(E) == len(T)
                if estimator == 'NelsonAalen':
                    fitter = NelsonAalenFitter()
                    fitter.fit(durations=T, event_observed=E)
                    estimate = fitter.cumulative_hazard_[
                        'NA_estimate'].tolist()
                    ci_lower = fitter.confidence_interval_[
                        'NA_estimate_lower_0.95'].tolist()
                    ci_upper = fitter.confidence_interval_[
                        'NA_estimate_upper_0.95'].tolist()
                elif estimator == 'KaplanMeier':
                    fitter = KaplanMeierFitter()
                    fitter.fit(durations=T, event_observed=E)
                    # noinspection PyUnresolvedReferences
                    estimate = fitter.survival_function_[
                        'KM_estimate'].tolist()
                    ci_lower = fitter.confidence_interval_[
                        'KM_estimate_lower_0.95'].tolist()
                    ci_upper = fitter.confidence_interval_[
                        'KM_estimate_upper_0.95'].tolist()
                else:
                    error = 'Unknown estimator: {}'.format(estimator)
                    logger.exception(error)
                    raise ValueError(error)
                timeline = fitter.timeline.tolist()
                if not stats.get(category):
                    stats[category] = {}
                stats[category][subset] = {
                    'timeline': timeline,
                    'estimate': estimate,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper
                }

        return {
            'label': df['feature'].tolist()[0],
            'categories': categories,
            'subsets': subsets,
            'stats': stats
        }

"""This module provides statistics for a Kaplan Meier Survival Analysis."""

import logging
from typing import List

import pandas as pd
import numpy as np
from lifelines import KaplanMeierFitter

from fractalis.analytics.task import AnalyticTask
from fractalis.analytics.tasks.shared import utils



logger = logging.getLogger(__name__)


class KaplanMeierSurvivalTask(AnalyticTask):
    """Kaplan Meier Survival Analysis Task implementing AnalyticTask. This
    class is a submittable celery task."""

    name = 'kaplan-meier-estimate'

    def main(self, durations: List[pd.DataFrame],
             categories: List[pd.DataFrame],
             event_observed: List[pd.DataFrame],
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
        if id_filter:
            df = df[df['id'].isin(id_filter)]
        df = utils.apply_subsets(df=df, subsets=subsets)
        df = utils.apply_categories(df=df, categories=categories)

        stats = {}
        # for every category and subset combination estimate the survival fun.
        for category in df['category'].unique().tolist():
            for subset in df['subset'].unique().tolist():
                kmf = KaplanMeierFitter()
                sub_df = df[(df['category'] == category) &
                            (df['subset'] == subset)]
                T = sub_df['value']
                E = None  # default is nothing is censored
                if event_observed:
                    # find observation boolean value for every duration
                    E = event_observed[0].merge(sub_df, how='left', on='id')
                    E = [bool(x) and not np.isnan(x) for x in E['value']]
                    assert len(E) == len(T)
                kmf.fit(durations=T, event_observed=E)
                if not stats.get(category):
                    stats[category] = {}
                # noinspection PyUnresolvedReferences
                stats[category][subset] = {
                    'timeline': kmf.timeline,
                    'median': kmf.median_,
                    'survival_function':
                        kmf.survival_function_.to_dict(orient='list'),
                    'confidence_interval':
                        kmf.confidence_interval_.to_dict(orient='list')
                }

        return {
            'stats': stats
        }

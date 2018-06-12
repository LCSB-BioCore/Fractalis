"""This module contains tests for the survival module."""

from lifelines.datasets import load_waltons

from fractalis.analytics.tasks.survival.main import SurvivalTask


class TestSurvivalTask:

    task = SurvivalTask()

    def test_correct_output_for_simple_input(self):
        df = load_waltons()
        df.insert(0, 'id', df.index)
        duration = df[['id', 'T']].copy()
        duration.insert(1, 'feature', 'duration')
        duration.columns.values[2] = 'value'
        results = self.task.main(durations=[duration],
                                 categories=[],
                                 event_observed=[],
                                 estimator='KaplanMeier',
                                 id_filter=[],
                                 subsets=[])
        assert results['stats'][''][0]['timeline']
        assert results['stats'][''][0]['estimate']
        assert results['stats'][''][0]['ci_lower']
        assert results['stats'][''][0]['ci_upper']

    def test_correct_output_for_complex_input(self):
        df = load_waltons()
        df.insert(0, 'id', df.index)
        duration = df[['id', 'T']].copy()
        duration.insert(1, 'feature', 'duration')
        duration.columns.values[2] = 'value'
        event_observed = df[['id', 'E']].copy()
        event_observed.insert(1, 'feature', 'was_observed')
        categories = df[['id', 'group']].copy()
        categories.insert(1, 'feature', 'group')
        results = self.task.main(durations=[duration],
                                 categories=[categories],
                                 event_observed=[event_observed],
                                 estimator='NelsonAalen',
                                 id_filter=[],
                                 subsets=[])
        assert results['stats']['control'][0]['timeline']
        assert results['stats']['control'][0]['estimate']
        assert results['stats']['control'][0]['ci_lower']
        assert results['stats']['control'][0]['ci_upper']
        assert results['stats']['miR-137'][0]['timeline']
        assert results['stats']['miR-137'][0]['estimate']
        assert results['stats']['miR-137'][0]['ci_lower']
        assert results['stats']['miR-137'][0]['ci_upper']

    def test_can_handle_nans(self):
        df = load_waltons()
        df.insert(0, 'id', df.index)
        duration = df[['id', 'T']].copy()
        duration.insert(1, 'feature', 'duration')
        duration.columns.values[2] = 'value'
        duration.loc[duration.index % 2 == 0, 'value'] = float('nan')
        self.task.main(durations=[duration],
                       categories=[],
                       event_observed=[],
                       estimator='KaplanMeier',
                       id_filter=[],
                       subsets=[])

    def test_can_handle_empty_groups(self):
        df = load_waltons()
        df.insert(0, 'id', df.index)
        df.loc[df['group'] == 'miR-137', 'T'] = float('nan')
        duration = df[['id', 'T']].copy()
        duration.insert(1, 'feature', 'duration')
        duration.columns.values[2] = 'value'

        results = self.task.main(durations=[duration],
                                 categories=[],
                                 event_observed=[],
                                 estimator='KaplanMeier',
                                 id_filter=[],
                                 subsets=[])
        assert 'control' not in results['stats']
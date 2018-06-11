"""This module contains tests for the kaplan_meier_survival module."""

from lifelines.datasets import load_waltons

from fractalis.analytics.tasks.kaplan_meier_survival.main \
    import KaplanMeierSurvivalTask


class TestKaplanMeierSurvivalTask:

    task = KaplanMeierSurvivalTask()

    def test_correct_output_for_simple_input(self):
        df = load_waltons()
        df.insert(0, 'id', df.index)
        duration = df[['id', 'T']].copy()
        duration.insert(1, 'feature', 'duration')
        duration.columns.values[2] = 'value'
        results = self.task.main(durations=[duration],
                                 categories=[],
                                 event_observed=[],
                                 id_filter=[],
                                 subsets=[])
        assert 'timeline' in results['stats'][''][0]
        assert 'median' in results['stats'][''][0]
        assert 'survival_function' in results['stats'][''][0]
        assert 'confidence_interval' in results['stats'][''][0]

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
                                 id_filter=[],
                                 subsets=[])
        assert results['stats']['control']
        assert results['stats']['miR-137']

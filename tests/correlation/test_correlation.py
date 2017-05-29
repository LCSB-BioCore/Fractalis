"""Test suite for correlation analysis."""
import json

import pytest
import pandas as pd
import numpy as np

from fractalis.analytics.tasks.correlation.main import CorrelationTask


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestCorrelation:

    def test_functional_1(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_2 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result = task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='pearson',
                           subsets=[list(range(20))],
                           annotations=[])
        assert result['coef']
        assert result['p_value']
        assert result['slope']
        assert result['intercept']
        assert result['subsets']
        assert result['method'] == 'pearson'
        assert result['data']
        assert result['x_label'] == 'A'
        assert result['y_label'] == 'B'

    def test_functional_2(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_2 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result = task.main(x=x,
                           y=y,
                           id_filter=list(range(10)),
                           method='pearson',
                           subsets=[list(range(5, 15))],
                           annotations=[])
        df = json.loads(result['data'])
        assert len(df['id']) == 5

    def test_functional_3(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_2 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result_1 = task.main(x=x,
                             y=y,
                             id_filter=list(range(20)),
                             method='pearson',
                             subsets=[],
                             annotations=[])
        result_2 = task.main(x=x,
                             y=y,
                             id_filter=list(range(20)),
                             method='pearson',
                             subsets=[list(range(20))],
                             annotations=[])
        assert result_1 == result_2

    def test_functional_4(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_2 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        with pytest.raises(ValueError):
            task.main(x=x,
                      y=y,
                      id_filter=[],
                      method='foo',
                      subsets=[list(range(20))],
                      annotations=[])

    def test_functional_5(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_2 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result = task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='pearson',
                           subsets=[list(range(15, 25))],
                           annotations=[])
        df = json.loads(result['data'])
        assert len(df['id']) == 5

    def test_functional_6(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(20), np.random.randint(0, 100, size=(20, 1))]
        arr_2 = np.c_[range(20, 40), np.random.randint(0, 100, size=(20, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        with pytest.raises(ValueError):
            task.main(x=x,
                      y=y,
                      id_filter=[],
                      method='pearson',
                      subsets=[list(range(20))],
                      annotations=[])

    def test_functional_7(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(10), np.random.randint(0, 100, size=(10, 1))]
        arr_2 = np.c_[range(5, 20), np.random.randint(0, 100, size=(15, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        with pytest.raises(ValueError):
            task.main(x=x,
                      y=y,
                      id_filter=[],
                      method='pearson',
                      subsets=[list(range(10, 20))],
                      annotations=[])

    def test_functional_8(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(10), np.random.randint(0, 100, size=(10, 1))]
        arr_2 = np.c_[range(5, 20), np.random.randint(0, 100, size=(15, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result = task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='pearson',
                           subsets=[list(range(5, 20))],
                           annotations=[])
        df = json.loads(result['data'])
        assert len(df['id']) == 5

    def test_functional_9(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(10), np.random.randint(0, 100, size=(10, 1))]
        arr_2 = np.c_[range(5, 20), np.random.randint(0, 100, size=(15, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result = task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='pearson',
                           subsets=[
                               list(range(5)),
                               list(range(5, 10)),
                               list(range(10, 20))
                           ],
                           annotations=[])
        assert not np.isnan(result['coef'])
        assert len(result['subsets']) == 3
        assert np.isnan(result['subsets'][0]['coef'])
        assert not np.isnan(result['subsets'][1]['coef'])
        assert np.isnan(result['subsets'][2]['coef'])

    def test_functional_10(self):
        task = CorrelationTask()
        arr_1 = np.c_[range(2), np.random.randint(0, 100, size=(2, 1))]
        arr_2 = np.c_[range(1, 3), np.random.randint(0, 100, size=(2, 1))]
        x = pd.DataFrame(arr_1, columns=['id', 'A'])
        y = pd.DataFrame(arr_2, columns=['id', 'B'])
        result = task.main(x=x,
                           y=y,
                           id_filter=[],
                           method='pearson',
                           subsets=[list(range(4))],
                           annotations=[])
        df = json.loads(result['data'])
        assert np.isnan(result['coef'])
        assert len(df['id']) == 1

    def test_merge_x_y_merges_correctly(self):
        task = CorrelationTask()
        df1 = pd.DataFrame([[1, 'a'], [2, float('nan')], [3, 'a']],
                           columns=['id', 'A'])
        df2 = pd.DataFrame([[1, 'b'], [2, 'b'], [4, 'b']],
                           columns=['id', 'B'])
        result = task.merge_x_y(df1, df2)
        assert result.shape[0] == 1
        assert list(result['id']) == [1]

    def test_apply_id_filter(self):
        task = CorrelationTask()
        df = pd.DataFrame([[1, 'a'], [2, 'a']], columns=['id', 'A'])
        result = task.apply_id_filter(df, [2, 3])
        assert result.shape[0] == 1
        assert list(result['id']) == [2]

    def test_apply_subsets(self):
        task = CorrelationTask()
        df = pd.DataFrame([[1, 'a'], [2, 'a'], [3, 'a']], columns=['id', 'A'])
        result = task.apply_subsets(df, [[1, 2], [], [3, 4], [5], [2]])
        assert list(result['id']) == [1, 2, 3, 2]
        assert list(result['subset']) == [0, 0, 2, 4]

    def test_apply_empty_subsets(self):
        task = CorrelationTask()
        df = pd.DataFrame([[1, 'a'], [2, 'a'], [3, 'a']], columns=['id', 'A'])
        result = task.apply_subsets(df, [])
        assert list(result['subset']) == [0, 0, 0]

    def test_apply_annotations(self):
        task = CorrelationTask()
        df = pd.DataFrame([[1, 'a'], [2, 'a'], [3, 'a']], columns=['id', 'A'])
        annotation_1 = pd.DataFrame([[1, 'x']], columns=['id', 'x'])
        annotation_2 = pd.DataFrame([[1, 'y'], [2, 'y', 'z']],
                                    columns=['id', 'y', 'z'])
        result = task.apply_annotations([annotation_1, annotation_2], df)
        assert list(result['annotation'])[0:2] == ['x&&y', 'y&&z']
        assert np.isnan(list(result['annotation'])[2])

    def test_compute_stats_1(self):
        task = CorrelationTask()
        df = pd.DataFrame([[1, 1, 2],
                           [2, 3, 4],
                           [3, 5, 6]], columns=['id', 'A', 'B'])
        result = task.compute_stats(df, 'A', 'B')
        assert not np.isnan(result['coef'])
        assert not np.isnan(result['p_value'])
        assert not np.isnan(result['slope'])
        assert not np.isnan(result['intercept'])

    def test_compute_stats_2(self):
        task = CorrelationTask()
        df = pd.DataFrame([[1, 1, 2],
                           [2, float('nan'), 4],
                           [3, 5, float('nan')]], columns=['id', 'A', 'B'])
        result = task.compute_stats(df, 'A', 'B')
        assert np.isnan(result['coef'])
        assert np.isnan(result['p_value'])
        assert np.isnan(result['slope'])
        assert np.isnan(result['intercept'])

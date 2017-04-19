import json

import pytest
import numpy as np
import pandas as pd

from fractalis.analytics.jobs.correlation.main import CorrelationJob


class TestCorrelation:

    def test_returns_valid_response(self):
        job = CorrelationJob()
        x = np.random.rand(10).tolist()
        y = np.random.rand(10).tolist()
        result = job.main(x=x, y=y)
        try:
            result = json.loads(result)
        except ValueError:
            assert False
        assert result.coef
        assert result.p_value
        assert result.slope
        assert result.intercept
        assert result.data
        assert result.x_label
        assert result.y_label

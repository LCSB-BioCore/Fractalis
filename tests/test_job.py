from time import sleep
from uuid import UUID, uuid4

import pytest

from fractalis.analytics import job


class TestJob(object):

    def test_exception_when_starting_non_existing_script(self):
        with pytest.raises(ImportError):
            job.create_job('querty', {})

    def test_exception_when_invalid_parameters(self):
        with pytest.raises(TypeError):
            job.create_job('test/sample/add', {'a': 1})

    def test_start_job_returns_uuid(self):
        job_id = job.create_job('test/sample/add', {})
        UUID(job_id)

    def test_job_in_progress_has_running_status(self):
        job_id = job.create_job('test/sample/do_nothing', {'time': 2})
        job_details = job.get_job_details(job_id)
        assert job_details.status == 'RUNNING'

    def test_finished_job_returns_results(self):
        job_id = job.create_job('test/sample/add', {'a': 1, 'b': 2})
        sleep(1)
        job_details = job.get_job_details(job_id)
        assert job_details.status == 'FINISHED'
        assert job_details.message == 3

    def test_exception_when_checking_non_existing_job(self):
        with pytest.raises(LookupError):
            job.get_job_details(uuid4())

    def test_job_is_gone_after_canceling(self):
        job_id = job.create_job('test/sample/do_nothing', {'time': 10})
        job.cancel_job(job_id)
        # TODO Not sure which exception is thrown
        with pytest.raises():
            job.get_job_details(job_id)

    def test_exception_when_canceling_non_existing_job(self):
        # TODO Not sure which exception is thrown
        with pytest.raises():
            job.cancel_job(uuid4())

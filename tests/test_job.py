import pytest


class TestJob(object):

    def test_exception_when_starting_non_existing_script(self):
        assert False

    def test_start_job_returns_uuid(self):
        assert False

    def test_delayed_job_has_pending_status(self):
        assert False

    def test_job_in_progress_has_running_status(self):
        assert False

    def test_finished_job_returns_results(self):
        assert False

    def test_exception_when_checking_non_existing_job(self):
        assert False

    def test_job_is_gone_after_canceling(self):
        assert False

    def test_exception_when_canceling_non_existing_job(self):
        assert False

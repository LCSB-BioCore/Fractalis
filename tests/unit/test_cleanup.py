"""This module provides tests for the janitor"""

import os
import json
from pathlib import Path
from shutil import rmtree

from fractalis.cleanup import janitor
from fractalis import app, redis, celery


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestCleanup:

    data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')

    def teardown_method(self, method):
        redis.flushall()
        rmtree(self.data_dir)

    def test_janitor_removes_untracked_files(self):
        os.makedirs(self.data_dir, exist_ok=True)
        Path(os.path.join(self.data_dir, 'abc')).touch()
        janitor()
        assert not os.path.exists(os.path.join(self.data_dir, 'abc'))

    def test_janitor_does_not_remove_tracked_files(self):
        os.makedirs(self.data_dir, exist_ok=True)
        Path(os.path.join(self.data_dir, 'abc')).touch()
        redis.set('data:abc', '')
        janitor()
        assert os.path.exists(os.path.join(self.data_dir, 'abc'))

    def test_janitor_removes_redis_keys_without_file(self, monkeypatch):
        os.makedirs(self.data_dir, exist_ok=True)
        path = os.path.join(self.data_dir, '123')
        data_state = {
            'file_path': path
        }
        redis.set('data:123', json.dumps(data_state))

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'SUCCESS'

            def get(self, *args, **kwargs):
                pass
        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        janitor()
        assert not redis.exists('data:123')

    def test_janitor_doesnt_remove_redis_keys_with_file(self, monkeypatch):
        os.makedirs(self.data_dir, exist_ok=True)
        path = os.path.join(self.data_dir, '123')
        Path(path).touch()
        data_state = {
            'file_path': path
        }
        redis.set('data:123', json.dumps(data_state))

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'SUCCESS'

            def get(self, *args, **kwargs):
                pass
        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        janitor()
        assert redis.exists('data:123')

    def test_janitor_doesnt_remove_redis_keys_if_not_finished(
            self, monkeypatch):
        os.makedirs(self.data_dir, exist_ok=True)
        path = os.path.join(self.data_dir, '123')
        data_state = {
            'file_path': path
        }
        redis.set('data:123', json.dumps(data_state))

        class FakeAsyncResult:
            def __init__(self, *args, **kwargs):
                self.state = 'SUBMITTED'

            def get(self, *args, **kwargs):
                pass
        monkeypatch.setattr(celery, 'AsyncResult', FakeAsyncResult)
        janitor()
        assert redis.exists('data:123')

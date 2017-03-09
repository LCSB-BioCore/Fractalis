"""This module provides several methods that help keeping the web service
components synchronized.
"""

import os
import json
import time
from pathlib import Path
from uuid import uuid4

import pytest

from fractalis import app, sync


class TestSync:

    @pytest.fixture()
    def redis(self):
        from fractalis import redis
        yield redis
        sync.cleanup_all()

    def test_expired_entries_removed(self, redis):
        data_obj = {
            'file_path': 'foo',
            'last_access': 0  # last access = 1970
        }
        redis.hset(name='data', key=123, value=json.dumps(data_obj))
        assert redis.hget(name='data', key=123)
        sync.remove_expired_redis_entries()
        assert not redis.hget(name='data', key=123)

    def test_not_expired_entries_not_removed(self, redis):
        data_obj = {
            'file_path': 'foo',
            'last_access': time.time()  # now
        }
        redis.hset(name='data', key=123, value=json.dumps(data_obj))
        assert redis.hget(name='data', key=123)
        sync.remove_expired_redis_entries()
        assert redis.hget(name='data', key=123)

    def test_expired_tracked_files_removed(self, redis):
        tmp_dir = app.config['FRACTALIS_TMP_DIR']
        data_dir = os.path.join(tmp_dir, 'data')
        file_path = os.path.join(data_dir, str(uuid4()))
        data_obj = {
            'file_path': file_path,
            'last_access': 0  # last access = 1970
        }
        os.makedirs(data_dir, exist_ok=True)
        Path(file_path).touch()
        redis.hset(name='data', key=123, value=json.dumps(data_obj))
        assert os.path.exists(file_path)
        sync.remove_expired_redis_entries()
        assert not os.path.exists(file_path)

    def test_not_expired_tracked_files_not_removed(self, redis):
        tmp_dir = app.config['FRACTALIS_TMP_DIR']
        data_dir = os.path.join(tmp_dir, 'data')
        file_path = os.path.join(data_dir, str(uuid4()))
        data_obj = {
            'file_path': file_path,
            'last_access': time.time()
        }
        os.makedirs(data_dir, exist_ok=True)
        Path(file_path).touch()
        redis.hset(name='data', key=123, value=json.dumps(data_obj))
        assert os.path.exists(file_path)
        sync.remove_expired_redis_entries()
        assert os.path.exists(file_path)

    def test_untracked_files_removed(self):
        tmp_dir = app.config['FRACTALIS_TMP_DIR']
        data_dir = os.path.join(tmp_dir, 'data')
        file_path = os.path.join(data_dir, str(uuid4()))
        os.makedirs(data_dir, exist_ok=True)
        Path(file_path).touch()
        assert os.path.exists(file_path)
        sync.remove_untracked_data_files()
        assert not os.path.exists(file_path)

    def test_tracked_files_not_removed(self, redis):
        tmp_dir = app.config['FRACTALIS_TMP_DIR']
        data_dir = os.path.join(tmp_dir, 'data')
        file_path = os.path.join(data_dir, str(uuid4()))
        data_obj = {
            'file_path': file_path
        }
        os.makedirs(data_dir, exist_ok=True)
        Path(file_path).touch()
        redis.hset(name='data', key=123, value=json.dumps(data_obj))
        assert os.path.exists(file_path)
        sync.remove_untracked_data_files()
        assert os.path.exists(file_path)

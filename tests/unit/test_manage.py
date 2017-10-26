"""This module provides tests for the janitor"""

import os
from pathlib import Path

import manage
from fractalis import app, redis


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestManage:

    def test_janitor_removes_untracked_files(self):
        tmp_dir = app.config['FRACTALIS_TMP_DIR']
        os.makedirs(tmp_dir, exist_ok=True)
        Path(os.path.join(tmp_dir, 'abc')).touch()
        manage.janitor()
        assert not os.path.exists(os.path.join(tmp_dir, 'abc'))

    def test_janitor_does_not_remove_tracked_files(self):
        tmp_dir = app.config['FRACTALIS_TMP_DIR']
        os.makedirs(tmp_dir, exist_ok=True)
        Path(os.path.join(tmp_dir, 'abc')).touch()
        redis.set('data:abc', '')
        manage.janitor()
        assert os.path.exists(os.path.join(tmp_dir, 'abc'))

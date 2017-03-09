"""This module provides functions and celery tasks used to cleanup expired
items. It is also used to keep several components synchronized, e.g. the redis
db and the file system.
"""

import os
import json
import datetime
from glob import iglob
from shutil import rmtree

from fractalis.celery import app as celery
from fractalis import redis
from fractalis import app


@celery.task
def remove_expired_redis_entries() -> None:
    """Remove 'data' entries from the redis DB for which 'last_access' lays back
    longer than the timedelta defined in 'FRACTALIS_CACHE_EXP'.
    """
    cache_expr = app.config['FRACTALIS_CACHE_EXP']
    redis_data = redis.hgetall(name='data')
    for key in redis_data:
        data_obj = json.loads(redis_data[key].decode('utf-8'))
        last_access = datetime.datetime.fromtimestamp(data_obj['last_access'])
        now = datetime.datetime.now()
        delta = now - last_access
        if delta > cache_expr:
            redis.hdel('data', key)
    remove_untracked_data_files()


@celery.task
def remove_untracked_data_files() -> None:
    """Remove files that have no record in the redis DB"""
    tmp_dir = app.config['FRACTALIS_TMP_DIR']
    data_dir = os.path.join(tmp_dir, 'data')
    redis_data = redis.hgetall('data')
    for file_path in iglob(os.path.join(data_dir, '*')):
        # check if file tracked by redis
        is_tracked = False
        for key in redis_data:
            data_obj = json.loads(redis_data[key].decode('utf-8'))
            if data_obj['file_path'] == file_path:
                is_tracked = True
                break
        if not is_tracked:
            os.remove(file_path)


def cleanup_all() -> None:
    """Reset redis and the filesystem. This is only useful for testing and
    should !!!NEVER!!! be used otherwise.
    """
    redis.flushall()
    tmp_dir = app.config['FRACTALIS_TMP_DIR']
    if os.path.exists(tmp_dir):
        rmtree(tmp_dir)

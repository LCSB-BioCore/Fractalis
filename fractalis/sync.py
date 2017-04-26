"""This module provides functions and celery tasks used to cleanup expired
items. It is also used to keep several components synchronized, e.g. the redis
db and the file system.
"""

import os
from shutil import rmtree

from fractalis import redis, app


def cleanup_all() -> None:
    """Reset redis and the filesystem. This is only useful for testing and
    should !!!NEVER!!! be used otherwise.
    """
    redis.flushall()
    tmp_dir = app.config['FRACTALIS_TMP_DIR']
    if os.path.exists(tmp_dir):
        rmtree(tmp_dir)

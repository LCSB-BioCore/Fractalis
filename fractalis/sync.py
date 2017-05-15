"""This module provides functions and celery tasks used to cleanup expired
items. It is also used to keep several components synchronized, e.g. the redis
db and the file system.
"""

import os
import json
import logging
from shutil import rmtree

from fractalis import redis, app, celery


logger = logging.getLogger(__name__)


@celery.task
def remove_data(task_id: str) -> None:
    """Remove all traces of any data associated with the given id. That includes
    redis and the file system.
    :param task_id: The id associated with a data state
    """
    key = 'data:{}'.format(task_id)
    value = redis.get(key)
    celery.control.revoke(task_id, terminate=True, signal='SIGUSR1')
    redis.delete(key)
    if value:
        data_state = json.loads(value.decode('utf-8'))
        remove_file.apply(args=[data_state['file_path']])
    else:
        logger.warning("Can't delete file for task id '{}',because there is "
                       "no associated entry in Redis.".format(task_id))


@celery.task
def remove_file(file_path: str) -> None:
    """Remove the file for the given file path.
    :param file_path: Path of file to remove.
    """
    try:
        os.remove(file_path)
    except FileNotFoundError:
        logger.warning("Attempted to remove file '{}', "
                       "but it does not exist.".format(file_path))


@celery.task
def cleanup_all() -> None:
    """Reset redis and the filesystem. This is only useful for testing and
    should !!!NEVER!!! be used otherwise.
    """
    celery.control.purge()
    redis.flushall()
    tmp_dir = app.config['FRACTALIS_TMP_DIR']
    if os.path.exists(tmp_dir):
        rmtree(tmp_dir)

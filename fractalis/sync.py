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


def remove_data(task_id: str, wait: bool=False) -> None:
    """Remove all traces of any data associated with the given id. That includes
    redis and the file system.
    :param task_id: The id associated with a data state
    :param wait: Wait for all subtasks to finish before returning
    """
    key = 'data:{}'.format(task_id)
    value = redis.get(key)
    celery.control.revoke(task_id, terminate=True, signal='SIGUSR1')
    redis.delete(key)
    if value:
        data_state = json.loads(value)
        async_result = remove_file.delay(data_state['file_path'])
        if wait:
            async_result.get(propagate=False)
    else:
        logger.warning("Can't delete file for task id '{}',because there is "
                       "no associated entry in Redis.".format(task_id))


@celery.task
def remove_file(file_path: str) -> None:
    """Remove the file for the given file path. This is a task because celery
    workers might not have the same file system than the web service.
    :param file_path: Path of file to remove.
    """
    try:
        os.remove(file_path)
    except FileNotFoundError:
        logger.warning("Attempted to remove file '{}', "
                       "but it does not exist.".format(file_path))


def cleanup_all() -> None:
    """Reset redis, celery and the filesystem. This is only useful for testing 
    and should !!!NEVER!!! be used for anything else.
    """
    celery.control.purge()
    for key in redis.keys('data:*'):
        value = redis.get(key)
        data_state = json.loads(value)
        celery.AsyncResult(data_state['task_id']).get(propagate=False)
        # celery.control.revoke(data_state['task_id'], terminate=True,
        #                       signal='SIGUSR1', wait=True)
    redis.flushall()
    tmp_dir = app.config['FRACTALIS_TMP_DIR']
    if os.path.exists(tmp_dir):
        rmtree(tmp_dir)
    assert len(redis.keys('*')) == 0
    assert not os.path.exists(tmp_dir)

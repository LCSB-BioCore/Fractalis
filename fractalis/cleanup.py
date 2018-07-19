import os

from fractalis import app, redis, sync, celery


@celery.task
def janitor():
    """Ideally this is maintained by a systemd service to cleanup redis and the
    file system while Fractalis is running.
    """
    data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
    if not os.path.exists(data_dir):
        for key in redis.scan_iter('data:*'):
            redis.delete(key)
        return
    cached_files = [f for f in os.listdir(data_dir)
                    if os.path.isfile(os.path.join(data_dir, f))]
    tracked_files = [key.split(':')[1] for key in redis.scan_iter('data:*')]

    # clean cached files
    for cached_file in cached_files:
        if cached_file not in tracked_files:
            sync.remove_file(os.path.join(data_dir, cached_file))

    # clean tracked files
    for task_id in tracked_files:
        path = os.path.join(data_dir, task_id)
        async_result = celery.AsyncResult(task_id)
        if async_result.state == 'SUCCESS' and not os.path.exists(path):
            redis.delete('data:{}'.format(task_id))

import os

from fractalis import app, redis, sync, celery


@celery.task
def janitor():
    """Ideally this is maintained by a systemd service to cleanup redis and the
    file system while Fractalis is running.
    """
    tmp_dir = app.config['FRACTALIS_TMP_DIR']
    tracked_files = [key.split(':')[1] for key in redis.scan_iter('data:*')]
    cached_files = [f for f in os.listdir(tmp_dir)
                    if os.path.isfile(os.path.join(tmp_dir, f))]
    for cached_file in cached_files:
        if cached_file not in tracked_files:
            sync.remove_file(os.path.join(tmp_dir, cached_file))

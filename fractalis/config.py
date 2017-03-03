import os
from datetime import timedelta

# DO NOT MODIFY THIS FILE!

# Flask
DEBUG = False
TESTING = False
REDIS_HOST = '127.0.0.1'
REDIS_PORT = '6379'
PERMANENT_SESSION_LIFETIME = timedelta(days=1)
# Celery
BROKER_URL = 'amqp://'
CELERY_RESULT_BACKEND = 'redis://{}:{}'.format(REDIS_HOST, REDIS_PORT)
CELERY_TASK_TIME_LIMIT = 60 * 10
CELERYBEAT_SCHEDULE = {
    'cleanup-every-hour': {
        'task': 'fractalis.data.sync.cleanup',
        'schedule': timedelta(hours=1),
    }
}

FRACTALIS_TMP_DIR = os.path.abspath(os.path.join(
    os.sep, 'tmp', 'fractalis'))
FRACTALIS_CACHE_EXP = timedelta(days=10)

# DO NOT MODIFY THIS FILE!

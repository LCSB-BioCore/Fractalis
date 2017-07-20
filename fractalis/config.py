import os
import logging
from uuid import uuid4
from datetime import timedelta

# DO NOT MODIFY THIS FILE DIRECTLY


# Flask
SECRET_KEY = 'OVERWRITE ME IN PRODUCTION!!!'
DEBUG = False
TESTING = False
REDIS_HOST = '127.0.0.1'
REDIS_PORT = '6379'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_REFRESH_EACH_REQUEST = True
PERMANENT_SESSION_LIFETIME = timedelta(days=1)

# Celery
BROKER_URL = 'amqp://'
CELERY_RESULT_BACKEND = 'redis://{}:{}'.format(REDIS_HOST, REDIS_PORT)
CELERYD_TASK_SOFT_TIME_LIMIT = 60 * 10
CELERY_TASK_RESULT_EXPIRES = timedelta(hours=1)
CELERYD_HIJACK_ROOT_LOGGER = False

# Fractalis
LOG_LEVEL = logging.INFO
LOG_FILE = os.path.join(os.sep, 'tmp', 'fractalis.log')
FRACTALIS_TMP_DIR = os.path.abspath(os.path.join(
    os.sep, 'tmp', 'fractalis'))
FRACTALIS_CACHE_EXP = timedelta(days=10)
FRACTALIS_ENCRYPT_CACHE = False


# DO NOT MODIFY THIS FILE DIRECTLY

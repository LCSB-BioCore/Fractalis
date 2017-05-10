import os
import logging
from uuid import uuid4
from datetime import timedelta

# DO NOT MODIFY THIS FILE DIRECTLY


# Flask
SECRET_KEY = str(uuid4())  # set me manually in production
DEBUG = False
TESTING = False
REDIS_HOST = '127.0.0.1'
REDIS_PORT = '6379'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_REFRESH_EACH_REQUEST = True
PERMANENT_SESSION_LIFETIME = timedelta(days=1)

# Flask-Session
SESSION_TYPE = 'redis'
SESSION_PERMANENT = True
SESSION_USE_SIGNER = False

# Celery
BROKER_URL = 'amqp://'
CELERYD_TASK_SOFT_TIME_LIMIT = 60 * 10
CELERYD_HIJACK_ROOT_LOGGER = False

# Fractalis
LOG_LEVEL = logging.INFO
LOG_FILE = os.path.join(os.sep, 'tmp', 'fractalis.log')
FRACTALIS_TMP_DIR = os.path.abspath(os.path.join(
    os.sep, 'tmp', 'fractalis'))
FRACTALIS_CACHE_EXP = timedelta(days=10)


# DO NOT MODIFY THIS FILE DIRECTLY

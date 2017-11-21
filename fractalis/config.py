import os
from datetime import timedelta

# DO NOT MODIFY THIS FILE DIRECTLY


# Flask
SECRET_KEY = 'OVERWRITE ME IN PRODUCTION!!!'
DEBUG = False
TESTING = False
REDIS_HOST = os.environ.get('REDIS_HOST') or '127.0.0.1'
REDIS_PORT = '6379'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_REFRESH_EACH_REQUEST = True
PERMANENT_SESSION_LIFETIME = timedelta(days=1)

# Celery
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST') or '127.0.0.1'
BROKER_URL = 'amqp://guest:guest@{}:5672//'.format(RABBITMQ_HOST)
CELERY_RESULT_BACKEND = 'redis://{}:{}'.format(REDIS_HOST, REDIS_PORT)
CELERYD_TASK_SOFT_TIME_LIMIT = 60 * 20
CELERYD_TASK_TIME_LIMIT = 60 * 30
CELERY_TASK_RESULT_EXPIRES = timedelta(hours=1)
CELERYD_HIJACK_ROOT_LOGGER = False

# Fractalis
FRACTALIS_TMP_DIR = os.path.abspath(os.path.join(os.sep, 'tmp', 'fractalis'))
FRACTALIS_CACHE_EXP = timedelta(days=10)
FRACTALIS_ENCRYPT_CACHE = False
FRACTALIS_LOG_CONFIG = os.path.join(os.path.dirname(__file__), 'logging.yaml')


# DO NOT MODIFY THIS FILE DIRECTLY

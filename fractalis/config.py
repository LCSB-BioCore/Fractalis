import os
from datetime import timedelta

# DO NOT MODIFY THIS FILE DIRECTLY


# Flask
SECRET_KEY = 'OVERWRITE ME IN PRODUCTION!!!'
DEBUG = False
TESTING = False
REDIS_HOST = 'localhost'
REDIS_PORT = '6379'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
SESSION_REFRESH_EACH_REQUEST = True

# Celery
BROKER_URL = 'amqp://guest:guest@localhost:5672//'
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
CELERYD_TASK_SOFT_TIME_LIMIT = 60 * 20
CELERYD_TASK_TIME_LIMIT = 60 * 30
CELERYD_HIJACK_ROOT_LOGGER = False

# Fractalis
# Location of cache and temporary files
FRACTALIS_TMP_DIR = os.path.abspath(os.path.join(os.sep, 'tmp', 'fractalis'))
# How long to store files in the cache
FRACTALIS_DATA_LIFETIME = timedelta(days=6)
# How long to keep analysis results (beware of high RAM usage)
FRACTALIS_RESULT_LIFETIME = timedelta(seconds=30)
# Should the Cache be encrypted? This might impact performance for little gain!
FRACTALIS_ENCRYPT_CACHE = False
# Location of your the log configuration file.
FRACTALIS_LOG_CONFIG = os.path.join(os.path.dirname(__file__), 'logging.yaml')
# Whether to verify the certs of https data sources
ETL_VERIFY_SSL_CERT = False

# DO NOT MODIFY THIS FILE DIRECTLY

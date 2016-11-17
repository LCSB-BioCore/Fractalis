""" This file contains the default settings for Fractalis.
"""

# DO NOT MODIFY THIS FILE!
DEBUG = False
TESTING = False
REDIS_HOST = '127.0.0.1'
REDIS_PORT = '6379'
CELERY_BROKER_URL = 'amqp://'
CELERY_RESULT_BACKEND = 'redis://127.0.0.1:6379'
# DO NOT MODIFY THIS FILE!

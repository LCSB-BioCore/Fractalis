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

# DO NOT MODIFY THIS FILE!


def file_to_dict(abs_path):
    config = {}
    with open(abs_path) as f:
        for line in f:
            (key, value) = [s.strip() for s in line.split('=')]
            config[key] = value
    return config

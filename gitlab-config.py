"""The only purpose of this file is to have a running CI test environment."""
REDIS_HOST = 'redis'

BROKER_URL = 'amqp://guest:guest@rabbitmq:5672//'
CELERY_RESULT_BACKEND = 'redis://redis:6379'

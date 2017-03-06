"""The only purpose of this file is to have a running CI test environment."""

REDIS_HOST = 'redis'

broker_url = 'amqp://guest:guest@rabbitmq:5672//'
result_backend = 'redis://redis:6379'

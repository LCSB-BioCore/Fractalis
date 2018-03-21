SECRET_KEY = 'OVERWRITE ME IN PRODUCTION!!!'
REDIS_HOST = 'redis'
BROKER_URL = 'amqp://guest:guest@rabbitmq:5672//'
CELERY_RESULT_BACKEND = 'redis://redis:6379'
ETL_VERIFY_SSL_CERT = False

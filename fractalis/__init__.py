"""Initalize Fractalis Flask app and configure it.

Modules in this package:
    - config -- Manages Fractalis Flask app configuration
"""
from flask import Flask

from fractalis.session import RedisSessionInterface
from fractalis.celery import init_celery
from fractalis.analytics.controllers import analytics_blueprint


app = Flask(__name__)
app.config.from_object('fractalis.config')
app.session_interface = RedisSessionInterface(app.config)
celery_app = init_celery(app)

app.register_blueprint(analytics_blueprint, url_prefix='/analytics')

if __name__ == '__main__':
    app.config.from_envvar('FRACTALIS_CONFIG')
    celery_app.worker_main(['worker', '--loglevel=DEBUG'])
    app.run()

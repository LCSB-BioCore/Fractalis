"""Initialize Fractalis Flask app and configure it.

Modules in this package:
    - config -- Manages Fractalis Flask app configuration
"""
import logging

from flask import Flask
from flask_cors import CORS
from redis import StrictRedis

from fractalis.session import RedisSessionInterface


app = Flask(__name__)
CORS(app, supports_credentials=True)  # allow everyone to submit requests

# Configure app
app.config.from_object('fractalis.config')
try:
    app.config.from_envvar('FRACTALIS_CONFIG')
    app.logger.info("FRACTALIS_CONFIG environment variable is set and was "
                    "applied to Flask app.")
except RuntimeError:
    app.logger.warning("FRACTALIS_CONFIG environment variable is not set. "
                       "Using defaults for Flask app.")

redis = StrictRedis(host=app.config['REDIS_HOST'],
                    port=app.config['REDIS_PORT'])
app.session_interface = RedisSessionInterface(redis)

from fractalis.celeryapp import make_celery, register_tasks  # noqa
celery = make_celery(app)

# register blueprints
from fractalis.analytics.controller import analytics_blueprint  # noqa
from fractalis.data.controller import data_blueprint  # noqa
app.register_blueprint(analytics_blueprint, url_prefix='/analytics')
app.register_blueprint(data_blueprint, url_prefix='/data')

register_tasks()

if __name__ == '__main__':
    handler = logging.handlers.TimedRotatingFileHandler('fractalis.log',
                                                        when='midnight',
                                                        backupCount=14)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.run()

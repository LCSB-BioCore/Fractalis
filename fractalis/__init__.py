"""Initialize Fractalis Flask app and configure it.

Modules in this package:
    - config -- Manages Fractalis Flask app configuration
"""
import logging

from flask import Flask
from flask_cors import CORS
from flask_session import Session
from redis import StrictRedis


app = Flask(__name__)

# allow everyone to submit requests
CORS(app, supports_credentials=True)
# Configure app with defaults
app.config.from_object('fractalis.config')
# Configure app with manually settings
try:
    app.config.from_envvar('FRACTALIS_CONFIG')
    app.logger.info("FRACTALIS_CONFIG environment variable is set and was "
                    "applied to Flask app.")
except RuntimeError:
    app.logger.warning("FRACTALIS_CONFIG environment variable is not set. "
                       "Using defaults for Flask app.")

# create a redis instance
redis = StrictRedis(host=app.config['REDIS_HOST'],
                    port=app.config['REDIS_PORT'])

# Configure app with composed configurations to save admin some work
app.config['SESSION_REDIS'] = redis
app.config['CELERY_RESULT_BACKEND'] = 'redis://{}:{}'.format(
        app.config['REDIS_HOST'], app.config['REDIS_PORT'])

# Set new session interface for app
Session(app)

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

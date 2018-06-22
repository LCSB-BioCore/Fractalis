"""Initialize Fractalis Flask app and configure it."""

import logging.config
import yaml
from flask import Flask
from flask_cors import CORS
from flask_request_id import RequestID
from flask_compress import Compress
from redis import StrictRedis

from fractalis.session import RedisSessionInterface

app = Flask(__name__)

# Configure app with defaults
app.config.from_object('fractalis.config')
# Configure app with manually settings
default_config = True
try:
    app.config.from_envvar('FRACTALIS_CONFIG')
    default_config = False
except RuntimeError:
    pass
app.config['PERMANENT_SESSION_LIFETIME'] =\
    app.config['FRACTALIS_DATA_LIFETIME']
app.config['CELERY_TASK_RESULT_EXPIRES'] =\
    app.config['FRACTALIS_DATA_LIFETIME']

# setup logging
with open(app.config['FRACTALIS_LOG_CONFIG'], 'rt') as f:
    log_config = yaml.safe_load(f.read())
logging.config.dictConfig(log_config)
log_level = logging._nameToLevel[log_config['handlers']['default']['level']]
logging.getLogger('werkzeug').setLevel(log_level)
log = logging.getLogger(__name__)

# we can't log this earlier because the logger depends on the loaded app config
if default_config:
    log.warning("Environment Variable FRACTALIS_CONFIG not set. Falling back "
                "to default settings. This is not a good idea in production!")

# Plugin that assigns every request an id
RequestID(app)

# Plugin that compresses all responses
Compress(app)

# create a redis instance
log.info("Creating Redis connection.")
redis = StrictRedis(host=app.config['REDIS_HOST'],
                    port=app.config['REDIS_PORT'],
                    charset='utf-8',
                    decode_responses=True)

# Set new session interface for app
log.info("Replacing default session interface.")
app.session_interface = RedisSessionInterface(redis, app)

# allow everyone to submit requests
log.info("Setting up CORS.")
CORS(app, supports_credentials=True)

# create celery app
log.info("Creating celery app.")
from fractalis.celeryapp import make_celery, register_tasks  # noqa: E402
celery = make_celery(app)

# register blueprints
from fractalis.analytics.controller import analytics_blueprint  # noqa: E402
from fractalis.data.controller import data_blueprint  # noqa: E402
from fractalis.misc.controller import misc_blueprint  # noqa: E402
from fractalis.state.controller import state_blueprint  # noqa: E402
log.info("Registering Flask blueprints.")
app.register_blueprint(analytics_blueprint, url_prefix='/analytics')
app.register_blueprint(data_blueprint, url_prefix='/data')
app.register_blueprint(misc_blueprint, url_prefix='/misc')
app.register_blueprint(state_blueprint, url_prefix='/state')

# registering all application celery tasks
log.info("Registering celery tasks.")
register_tasks()

log.info("Initialisation of service complete.")

if __name__ == '__main__':
    log.info("Starting builtin web server.")
    app.run(host='0.0.0.0', port=5000)
    log.info("Builtin web server started.")

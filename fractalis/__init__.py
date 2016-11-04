"""Initalize Fractalis Flask app and configure it.

Modules in this package:
    - config -- Manages Fractalis Flask app configuration
"""
from flask import Flask

from fractalis.config import configure_app
from fractalis.celery import init_celery
from fractalis.analytics.controllers import analytics

flask_app = Flask(__name__)
configure_app(flask_app)
celery_app = init_celery(flask_app)

flask_app.register_blueprint(analytics, url_prefix='/analytics')

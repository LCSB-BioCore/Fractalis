"""Initalize Fractalis Flask app and configure it.

Modules in this package:
    - config -- Manages Fractalis Flask app configuration
"""
from flask import Flask

from fractalis.session import RedisSessionInterface
from fractalis.analytics.controllers import analytics_blueprint


app = Flask(__name__)
app.config.from_object('fractalis.config')
# app.config.from_envvar('FRACTALIS_CONFIG')
app.session_interface = RedisSessionInterface(app.config)
app.register_blueprint(analytics_blueprint, url_prefix='/analytics')

if __name__ == '__main__':
    app.run()

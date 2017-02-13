"""This module is responsible for the establishment and configuration of a
Celery instance."""
import os
import sys
import logging

from celery import Celery


def get_scripts_packages():
    packages = []
    script_dir = os.path.join(
        os.path.dirname(__file__), 'analytics', 'scripts')
    for dir_path, dir_names, file_names in os.walk(script_dir):
        if (dir_path == script_dir or '__pycache__' in dir_path or
                '__init__.py' not in file_names):
            continue
        dirname = os.path.basename(dir_path)
        package = 'fractalis.analytics.scripts.{}'.format(dirname)
        packages.append(package)
    return packages


app = Celery(__name__)
app.config_from_object('fractalis.config')
try:
    config_file = os.environ['FRACTALIS_CONFIG']
    module = os.path.splitext(os.path.basename(config_file))[0]
    sys.path.append(os.path.dirname(os.path.expanduser(config_file)))
    config = __import__(module).__dict__
    for key in app.conf:
        if key in config and key[0] != '_':
            app.conf[key] = config[key]
except KeyError:
    logger = logging.getLogger('fractalis')
    logger.warning("FRACTALIS_CONFIG is not set. Using defaults.")
app.autodiscover_tasks(packages=get_scripts_packages())

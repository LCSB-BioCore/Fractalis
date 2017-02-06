"""This module is responsible for the establishment and configuration of a
Celery instance."""
import os
import logging

from celery import Celery

from fractalis.config import file_to_dict


def get_scripts_packages():
    packages = []
    script_dir = os.path.join(
        os.path.dirname(__file__), 'analytics', 'scripts')
    for dirpath, dirnames, filenames in os.walk(script_dir):
        if (dirpath == script_dir or '__pycache__' in dirpath or
                '__init__.py' not in filenames):
            continue
        dirname = os.path.basename(dirpath)
        package = 'fractalis.analytics.scripts.{}'.format(dirname)
        packages.append(package)
    return packages


app = Celery(__name__)
app.config_from_object('fractalis.config')
try:
    config = file_to_dict(os.environ['FRACTALIS_CONFIG'])
    app.conf.update(config)
except KeyError:
    logger = logging.getLogger('fractalis')
    logger.warning("FRACTALIS_CONFIG is not set. Using defaults.")
app.autodiscover_tasks(packages=get_scripts_packages())

"""This module is responsible for the establishment and configuration of a
Celery instance."""
import os
import sys
import logging

from celery import Celery

from fractalis.utils import get_sub_packages_for_package

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

task_package = 'fractalis.analytics.scripts'
app.autodiscover_tasks(packages=get_sub_packages_for_package(task_package))

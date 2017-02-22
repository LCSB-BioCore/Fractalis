"""This module is responsible for the establishment and configuration of a
Celery instance."""
import os
import sys
import logging

from celery import Celery

from fractalis.utils import list_classes_with_base_class
from fractalis.data.etls.etl import ETL
from fractalis.analytics.job import AnalyticsJob


app = Celery(__name__)
app.config_from_object('fractalis.config')
try:
    config_file = os.environ['FRACTALIS_CONFIG']
    module = os.path.splitext(os.path.basename(config_file))[0]
    sys.path.append(os.path.dirname(os.path.expanduser(config_file)))
    config = __import__(module).__dict__
    for key in app.conf:
        if key in config and key.startswith('_'):
            app.conf[key] = config[key]
except KeyError:
    logger = logging.getLogger('fractalis')
    logger.warning("FRACTALIS_CONFIG is not set. Using defaults.")

etl_classes = list_classes_with_base_class('fractalis.data.etls', ETL)
for etl_class in etl_classes:
    app.tasks.register(etl_class)

analytics_job_classes = list_classes_with_base_class('fractalis.analytics.job',
                                                     AnalyticsJob)
for analytics_job_class in analytics_job_classes:
    app.tasks.register(analytics_job_class)

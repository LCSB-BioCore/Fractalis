"""This module is responsible for the establishment and configuration of a
Celery instance."""
import os
import logging

from celery import Celery

from fractalis.utils import list_classes_with_base_class
from fractalis.utils import import_module_by_abs_path
from fractalis.data.etl import ETL
from fractalis.analytics.job import AnalyticsJob


app = Celery(__name__)
app.config_from_object('fractalis.config')
try:
    module = import_module_by_abs_path(os.environ['FRACTALIS_CONFIG'])
    for key in app.conf:
        if key in module.__dict__ and not key.startswith('_'):
            app.conf[key] = module.__dict__[key]
except KeyError:
    logger = logging.getLogger('fractalis')
    logger.warning("FRACTALIS_CONFIG is not set. Using defaults.")

from fractalis.data.sync import cleanup
app.tasks.register(cleanup)

etl_classes = list_classes_with_base_class('fractalis.data.etls', ETL)
for etl_class in etl_classes:
    app.tasks.register(etl_class)

analytics_job_classes = list_classes_with_base_class('fractalis.analytics.job',
                                                     AnalyticsJob)
for analytics_job_class in analytics_job_classes:
    app.tasks.register(analytics_job_class)

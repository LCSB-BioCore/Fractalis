"""This module is responsible for the establishment and configuration of a
Celery instance."""

from celery import Celery

from fractalis.analytics.job import AnalyticsJob
from fractalis.data.etl import ETL
from fractalis.utils import list_classes_with_base_class


def make_celery(app):
    celery = Celery(app.import_name,
                    backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['BROKER_URL'])
    celery.conf.update(app.config)

    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask

    return celery


def register_tasks():
    from fractalis import celery
    from fractalis.sync import remove_untracked_data_files
    from fractalis.sync import remove_expired_redis_entries

    celery.tasks.register(remove_untracked_data_files)
    celery.tasks.register(remove_expired_redis_entries)

    etl_classes = list_classes_with_base_class('fractalis.data.etls', ETL)
    for etl_class in etl_classes:
        celery.tasks.register(etl_class)

    analytics_job_classes = list_classes_with_base_class(
        'fractalis.analytics.job', AnalyticsJob)
    for analytics_job_class in analytics_job_classes:
        celery.tasks.register(analytics_job_class)




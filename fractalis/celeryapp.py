"""This module is responsible for the establishment and configuration of a
Celery instance."""

import logging

from celery import Celery, current_app
from celery.signals import after_task_publish
from flask import Flask

from fractalis.utils import list_classes_with_base_class


logger = logging.getLogger(__name__)


# https://stackoverflow.com/questions/9824172/find-out-whether-celery-task-exists
@after_task_publish.connect
def update_submitted_state(sender, headers, **kwargs):
    """Add 'SUBMITTED' state to celery task."""
    # the task may not exist if sent using `send_task` which
    # sends tasks by name, so fall back to the default result backend
    # if that is the case.
    task = current_app.tasks.get(sender)
    backend = task.backend if task else current_app.backend
    backend.store_result(task_id=headers['id'],
                         result=None,
                         state='SUBMITTED')


def make_celery(app: Flask) -> Celery:
    """Create a celery instance which executes its tasks in the application
    context of our service.
    :param app: The instance of our web service. This holds our configuration.
    :return A celery instance that can submit tasks.
    """
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


def register_tasks() -> None:
    """Register all of our Task classes with celery."""
    from fractalis import celery
    from fractalis.analytics.task import AnalyticTask
    from fractalis.data.etl import ETL

    logger.info("Registering ETLs ...")
    etl_classes = list_classes_with_base_class('fractalis.data.etls', ETL)
    for etl_class in etl_classes:
        logger.info("Registering task: {}".format(etl_class.name))
        celery.tasks.register(etl_class)

    logger.info("Registering analysis tasks ...")
    analytics_task_classes = list_classes_with_base_class(
        'fractalis.analytics.task', AnalyticTask)
    for analytics_task_class in analytics_task_classes:
        logger.info("Registering task: {}".format(analytics_task_class.name))
        celery.tasks.register(analytics_task_class)

"""

"""
import fractalis.analytics.scripts # flake8: noqa


def get_celery_task(task):
    celery_task = eval('fractalis.analytics.scripts.{}'.format(task))
    return celery_task


def start_job(task, arguments):
    celery_task = get_celery_task(task)
    async_result = celery_task.delay(**arguments)
    return async_result.id


def cancel_job(task, job_id):
    pass


def get_job_result(task, job_id):
    celery_task = get_celery_task(task)
    return celery_task.AsyncResult(job_id)

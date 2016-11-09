"""

"""


def get_celery_task(script):
    split = script.split('.')
    module = 'fractalis.analytics.scripts.{}'.format(
        '.'.join(split[:-1]))
    exec('import {}'.format(module))
    celery_task = eval('{}.{}'.format(module, split[-1]))
    return celery_task


def start_job(script, arguments):
    celery_task = get_celery_task(script)
    async_result = celery_task.delay(**arguments)
    return async_result.id


def cancel_job(script, job_id):
    pass


def get_job_result(script, job_id):
    celery_task = get_celery_task(script)
    return celery_task.AsyncResult(job_id)

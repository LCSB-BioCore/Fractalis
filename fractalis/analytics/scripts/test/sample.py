from time import sleep

from fractalis import celery_app


@celery_app.task
def add(a, b):
    return a + b


@celery_app.task
def do_nothing(time):
    sleep(time)

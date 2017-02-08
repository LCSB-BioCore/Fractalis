import time

from fractalis.celery import app


@app.task
def add(a, b):
    return a + b


@app.task
def do_nothing(seconds):
    time.sleep(seconds)
    return 1


@app.task
def div(a, b):
    return a / b

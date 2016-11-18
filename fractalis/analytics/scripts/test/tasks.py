from time import sleep

from fractalis.celery import app


@app.task
def add(a, b):
    return a + b


@app.task
def do_nothing(time):
    sleep(time)


@app.task
def div(a, b):
    return a / b

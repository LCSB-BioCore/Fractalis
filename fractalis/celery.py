"""This module is responsible for the establishment and configuration of a
Celery instance."""
from celery import Celery


def init_celery(app):
    """Establish connection to celery broker and result backend.

    TThe function creates a new Celery object, configures it with the broker
    from the application config, updates the rest of the Celery config from the
    Flask config and then creates a subclass of the task that wraps the task
    execution in an application context.

    Arguments:
    app (Flask) -- An instance of Flask

    Exceptions:
    ConnectionRefusedError (Exception) -- Is raised when connection fails

    Returns:
    (Celery) -- An instance of Celery
    """
    celery = Celery(app.import_name,
                    backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
#    try:
#        celery.connection().connect()
#    except OSError as e:
#        error_msg = """Could not establish connection to celery broker.
#                    URL: '{}'""".format(app.config['CELERY_BROKER_URL'])
#        raise ConnectionRefusedError(error_msg) from e
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

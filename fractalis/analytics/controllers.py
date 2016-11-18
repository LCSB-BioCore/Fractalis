import json

from flask import Blueprint, abort

import fractalis.analytics.scripts  # noqa
from fractalis.celery import app
from fractalis.analytics.form import POSTAnalyticsForm


analytics_blueprint = Blueprint('analytics_blueprint', __name__)


def get_celery_task(task):
    task = eval('fractalis.analytics.scripts.{}'.format(task))
    return task


@analytics_blueprint.route('', methods=['POST'])
def create_job():
    form = POSTAnalyticsForm()
    if not form.validate():
        abort(400)


@analytics_blueprint.route('/<uuid:job_id>', methods=['GET'])
def get_job_details(job_id):
    pass


@analytics_blueprint.route('/<uuid:job_id>', methods=['DELETE'])
def cancel_job(job_id):
    pass

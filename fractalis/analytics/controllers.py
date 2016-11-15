import json
import uuid

from flask import Blueprint

import fractalis.analytics.scripts
from fractalis import celery_app

analytics_blueprint = Blueprint('analytics_blueprint', __name__)


def get_celery_task(task):
    celery_task = eval('fractalis.analytics.scripts.{}'.format(task))
    return celery_task


@analytics_blueprint.route('', methods=['POST'])
def create_job():
    body = json.dumps({'job_id': str(uuid.uuid4())})
    return body, 201


@analytics_blueprint.route('/<uuid:job_id>', methods=['GET'])
def get_job_details(job_id):
    pass


@analytics_blueprint.route('/<uuid:job_id>', methods=['DELETE'])
def cancel_job(job_id):
    pass

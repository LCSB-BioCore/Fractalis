import json
import uuid

from flask import Blueprint


analytics_blueprint = Blueprint('analytics_blueprint', __name__)


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

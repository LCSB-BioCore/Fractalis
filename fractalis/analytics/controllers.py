import json

from flask import Blueprint


analytics = Blueprint('analytics', __name__)


@analytics.route('', methods=['POST'])
def create_job():
    body = json.dumps({'job_id': 123})
    return body, 201


@analytics.route('/<uuid:job_id>', methods=['GET'])
def get_job_details(job_id):
    pass


@analytics.route('/<uuid:job_id>', methods=['DELETE'])
def cancel_job(job_id):
    pass

from flask import Blueprint, abort, session, request, jsonify

import fractalis.analytics.scripts  # noqa
from fractalis.celery import app as celery
from fractalis.validator import validate_json, validate_schema
from fractalis.analytics.schema import create_job_schema


analytics_blueprint = Blueprint('analytics_blueprint', __name__)


def get_celery_task(task):
    try:
        split = task.split('.')
        assert len(split) == 2, "Task should have the format 'package.task'"
        task = eval('fractalis.analytics.scripts.{}.tasks.{}'.format(*split))
    except AttributeError:
        return None
    return task


@analytics_blueprint.before_request
def permanent_session():
    session.permanent = True


@analytics_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_job_schema)
def create_job():
    task = get_celery_task(request.json['task'])
    if task is None:
        return jsonify({'error': 'Task {} not found.'.format(
            request.json['task'])}), 400
    try:
        async_result = task.delay(*request.json['arguments'])
    except TypeError as e:
        return jsonify({'error': 'Invalid Arguments for task {}: {}'.format(
                request.json['task'], e)}), 400

    if 'jobs' not in session:
        session['jobs'] = []
    session['jobs'].append(async_result.id)
    return jsonify({'job_id': async_result.id}), 201


@analytics_blueprint.route('/<uuid:job_id>', methods=['GET'])
def get_job_details(job_id):
    async_result = celery.AsyncResult(request.json['uuid'])
    return jsonify({'status': async_result.state}), 200


@analytics_blueprint.route('/<uuid:job_id>', methods=['DELETE'])
def cancel_job(job_id):
    pass

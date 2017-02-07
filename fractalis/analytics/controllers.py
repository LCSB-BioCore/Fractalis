from flask import Blueprint, session, request, jsonify

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
    json = request.get_json(force=True)
    task = get_celery_task(json['task'])
    if task is None:
        return jsonify({'error': 'Task {} not found.'.format(
            json['task'])}), 400
    try:
        async_result = task.delay(**json['args'])
    except TypeError as e:
        return jsonify({'error': 'Invalid Arguments for task {}: {}'.format(
                json['task'], e)}), 400

    if 'tasks' not in session:
        session['tasks'] = []
    session['tasks'].append(async_result.id)
    return jsonify({'task_id': async_result.id}), 201


@analytics_blueprint.route('/<uuid:task_id>', methods=['GET'])
def get_job_details(task_id):
    task_id = str(task_id)
    if (task_id not in session['tasks']):  # access control
        return jsonify({'error': "No matching task found."}), 404
    async_result = celery.AsyncResult(task_id)
    state = async_result.state
    result = async_result.result
    if (isinstance(result, Exception)):
        result = "{}: {}".format(type(result).__name__, str(result))
    return jsonify({'status': state,
                    'result': result}), 200


@analytics_blueprint.route('/<uuid:task_id>', methods=['DELETE'])
def cancel_job(task_id):
    task_id = str(task_id)
    if (task_id not in session['tasks']):
        return jsonify({'error': "No matchin task found."}), 404
    celery.control.revoke(task_id, terminate=True)
    session['tasks'].remove(task_id)
    return jsonify({'task_id': task_id}), 200

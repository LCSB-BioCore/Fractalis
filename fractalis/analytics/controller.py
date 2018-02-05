"""The /analytics controller. Please refer to doc/api for more information."""

import logging
from typing import Tuple
from uuid import UUID

from flask import Blueprint, session, request, jsonify
from flask.wrappers import Response

from fractalis import celery, app
from fractalis.validator import validate_json, validate_schema
from fractalis.analytics.schema import create_task_schema
from fractalis.analytics.task import AnalyticTask


analytics_blueprint = Blueprint('analytics_blueprint', __name__)
logger = logging.getLogger(__name__)


@analytics_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_task_schema)
def create_task() -> Tuple[Response, int]:
    """Create a new analytics task based on the parameters in the POST body.
    See doc/api/ for more information.
    :return: Flask Response
    """
    logger.debug("Received POST request on /analytics.")
    json = request.get_json(force=True)
    analytic_task = AnalyticTask.factory(json['task_name'])
    if analytic_task is None:
        logger.error("Could not submit task for unknown task name: "
                     "'{}'".format(json['task_name']))
        return jsonify({'error_msg': "Task with name '{}' not found."
                       .format(json['task_name'])}), 400
    async_result = analytic_task.delay(
        session_data_tasks=session['data_tasks'], args=json['args'],
        decrypt=app.config['FRACTALIS_ENCRYPT_CACHE'])
    session['analytic_tasks'].append(async_result.id)
    logger.debug("Task successfully submitted. Sending response.")
    return jsonify({'task_id': async_result.id}), 201


@analytics_blueprint.route('/<uuid:task_id>', methods=['GET'])
def get_task_details(task_id: UUID) -> Tuple[Response, int]:
    """Get task details for the given task_id.
     See doc/api/ for more information.
    :param task_id: ID returned on task creation.
    :return: Flask Response
    """
    logger.debug("Received GET request on /analytics/task_id.")
    wait = request.args.get('wait') == '1'
    task_id = str(task_id)
    if task_id not in session['analytic_tasks']:
        error = "Task ID '{}' not found in session. " \
                "Refusing access.".format(task_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    async_result = celery.AsyncResult(task_id)
    if wait and async_result.state == 'SUBMITTED':
        async_result.get(propagate=False)
    result = async_result.result
    if isinstance(result, Exception):  # Exception -> str
        result = "{}: {}".format(type(result).__name__, str(result))
    logger.debug("Task found and has access. Sending response.")
    return jsonify({'state': async_result.state, 'result': result}), 200


@analytics_blueprint.route('/<uuid:task_id>', methods=['DELETE'])
def cancel_task(task_id: UUID) -> Tuple[Response, int]:
    """Cancel a task for a given task_id.
    See doc/api/ for more information.
    :param task_id: ID returned on task creation.
    :return: Flask Response
    """
    logger.debug("Received DELETE request on /analytics/task_id.")
    task_id = str(task_id)
    if task_id not in session['analytic_tasks']:
        error = "Task ID '{}' not found in session. " \
                "Refusing access.".format(task_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    wait = request.args.get('wait') == '1'
    # possibly dangerous: http://stackoverflow.com/a/29627549
    celery.control.revoke(task_id, terminate=True, signal='SIGUSR1', wait=wait)
    session['analytic_tasks'].remove(task_id)
    logger.debug("Successfully send term signal to task. Sending response.")
    return jsonify(''), 200

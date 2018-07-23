"""The /data controller. Please refer to doc/api for more information."""

import json
import logging
from typing import Tuple, Union
from uuid import UUID

from flask import Blueprint, session, request, jsonify, Response

from fractalis.data.etlhandler import ETLHandler
from fractalis.data.schema import create_data_schema
from fractalis.validator import validate_json, validate_schema
from fractalis import celery, redis
from fractalis.sync import remove_data


data_blueprint = Blueprint('data_blueprint', __name__)
logger = logging.getLogger(__name__)


@data_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_data_schema)
def create_data_task() -> Tuple[Response, int]:
    """Submit new ETL tasks based on the payload of the request body.
    See doc/api/ for more information.
    :return: Empty response. Everything important is stored in the session.
    """
    logger.debug("Received POST request on /data.")
    wait = request.args.get('wait') == '1'
    payload = request.get_json(force=True)
    etl_handler = ETLHandler.factory(handler=payload['handler'],
                                     server=payload['server'],
                                     auth=payload['auth'])
    task_ids = etl_handler.handle(descriptors=payload['descriptors'],
                                  data_tasks=session['data_tasks'],
                                  use_existing=False,
                                  wait=wait)
    session['data_tasks'] += task_ids
    session['data_tasks'] = list(set(session['data_tasks']))
    logger.debug("Tasks successfully submitted. Sending response.")
    return jsonify(''), 201


def get_data_state_for_task_id(task_id: str, wait: bool) -> Union[dict, None]:
    """Return data state associated with task id.
    :param task_id: The id associated with the ETL task.
    :param wait: If true and ETL is still running wait for it.
    :return: Data state that has been stored in Redis.
    """
    async_result = celery.AsyncResult(task_id)
    if wait and async_result.state == 'SUBMITTED':
        logger.debug("'wait' was set. Waiting for tasks to finish ...")
        async_result.get(propagate=False)
    value = redis.get('data:{}'.format(task_id))
    if not value:
        return None
    data_state = json.loads(value)
    # add additional information to data_state
    result = async_result.result
    if isinstance(result, Exception):  # Exception -> str
        result = "{}: {}".format(type(result).__name__, str(result))
    data_state['etl_message'] = result
    data_state['etl_state'] = async_result.state
    return data_state


@data_blueprint.route('', methods=['GET'])
def get_all_data() -> Tuple[Response, int]:
    """Get information for all tasks that have been submitted in the lifetime
    of the current session.
    See doc/api/ for more information.
    :return: Information associated with each submitted task
    """
    logger.debug("Received GET request on /data.")
    wait = request.args.get('wait') == '1'
    data_states = []
    existing_data_tasks = []
    for task_id in session['data_tasks']:
        data_state = get_data_state_for_task_id(task_id, wait)
        if data_state is None:
            warning = "Data state with task_id '{}' expired. " \
                      "Discarding...".format(task_id)
            logger.warning(warning)
            continue
        # remove internal information from response
        del data_state['file_path']
        del data_state['meta']
        # add additional information to response
        data_states.append(data_state)
        existing_data_tasks.append(task_id)
    session['data_tasks'] = existing_data_tasks
    logger.debug("Data states collected. Sending response.")
    return jsonify({'data_states': data_states}), 200


@data_blueprint.route('/<uuid:task_id>', methods=['DELETE'])
def delete_data(task_id: UUID) -> Tuple[Response, int]:
    """Remove all traces of the data associated with the given task id.
    :param task_id: The id associated with the data
    See doc/api/ for more information.
    :return: Empty response.
    """
    logger.debug("Received DELETE request on /data/task_id.")
    wait = request.args.get('wait') == '1'
    task_id = str(task_id)
    if task_id not in session['data_tasks']:
        error = "Task ID '{}' not found in session. " \
                "Refusing access.".format(task_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    session['data_tasks'].remove(task_id)
    # possibly dangerous: http://stackoverflow.com/a/29627549
    celery.control.revoke(task_id, terminate=True, signal='SIGUSR1', wait=wait)
    remove_data(task_id=task_id)
    logger.debug("Successfully removed data from session. Sending response.")
    return jsonify(''), 200


@data_blueprint.route('', methods=['DELETE'])
def delete_all_data() -> Tuple[Response, int]:
    """Remove all traces of all data associated with this session.
    :return: Empty response.
    """
    logger.debug("Received DELETE request on /data.")
    wait = request.args.get('wait') == '1'
    for task_id in session['data_tasks']:
        remove_data(task_id=task_id)
        # possibly dangerous: http://stackoverflow.com/a/29627549
        celery.control.revoke(task_id, terminate=True,
                              signal='SIGUSR1', wait=wait)
    session['data_tasks'] = []
    logger.debug("Successfully removed all data from session. "
                 "Sending response.")
    return jsonify(''), 200


@data_blueprint.route('/meta/<uuid:task_id>', methods=['GET'])
def get_meta_information(task_id: UUID) -> Tuple[Response, int]:
    """Get meta information for given task id.
    :return: meta information object stored in redis.
    """
    logger.debug("Received GET request on /data/meta/task_id.")
    wait = request.args.get('wait') == '1'
    task_id = str(task_id)
    if task_id not in session['data_tasks']:
        error = "Task ID '{}' not found in session. " \
                "Refusing access.".format(task_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    data_state = get_data_state_for_task_id(task_id, wait)
    if data_state is None:
        error = "Could not find redis entry for this task id '{}'. " \
                "The entry probably expired.".format(task_id)
        logger.error(error)
        return jsonify({'error': error}), 404
    logger.debug("Successfully gather meta information. Sending response.")
    return jsonify({'meta': data_state['meta']}), 200

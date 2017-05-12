"""The /data controller. Please refer to doc/api for more information."""

import json
import logging
from typing import Tuple

from flask import Blueprint, session, request, jsonify
from flask.wrappers import Response

from fractalis.data.etlhandler import ETLHandler
from fractalis.data.schema import create_data_schema
from fractalis.validator import validate_json, validate_schema
from fractalis import celery, redis
from fractalis.sync import remove_data


data_blueprint = Blueprint('data_blueprint', __name__)
logger = logging.getLogger(__name__)


@data_blueprint.before_request
def prepare_session() -> None:
    """Make sure the session is properly initialized before each request."""
    session.permanent = True
    if 'data_tasks' not in session:
        logger.debug("Initializing data_tasks field in session dict.")
        session['data_tasks'] = []


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
    payload = request.get_json(force=True)  # pattern enforced by decorators
    etl_handler = ETLHandler.factory(handler=payload['handler'],
                                     server=payload['server'],
                                     auth=payload['auth'])
    task_ids = etl_handler.handle(descriptors=payload['descriptors'], wait=wait)
    session['data_tasks'] += task_ids
    session['data_tasks'] = list(set(session['data_tasks']))
    logger.debug("Tasks successfully submitted. Sending response.")
    return jsonify(''), 201


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
    for task_id in session['data_tasks']:
        async_result = celery.AsyncResult(task_id)
        if wait:
            logger.debug("'wait' was set. Waiting for tasks to finish ...")
            async_result.get(propagate=False)
        value = redis.get('data:{}'.format(task_id))
        if not value:
            error = "Could not find data entry in Redis for task_id: " \
                    "'{}'. The entry probably expired.".format(task_id)
            logger.warning(error)
            continue
        data_state = json.loads(value.decode('utf-8'))
        # remove internal information from response
        del data_state['file_path']
        del data_state['last_access']
        # add additional information to response
        data_state['etl_state'] = async_result.state
        data_state['etl_message'] = async_result.result
        data_states.append(data_state)
    logger.debug("Data states collected. Sending response.")
    return jsonify({'data_states': data_states}), 200


@data_blueprint.route('/<string:task_id>', methods=['DELETE'])
def delete_data(task_id: str) -> Tuple[Response, int]:
    """Remove all traces of the data associated with the given task id.
    :param task_id: The id associated with the data
    See doc/api/ for more information.
    :return: Empty response.
    """
    logger.debug("Received DELETE request on /data/task_id.")
    if task_id in session['data_tasks']:
        session['data_tasks'].remove(task_id)
    remove_data.delay(task_id)
    logger.debug("Successfully removed data from session. Sending response.")
    return jsonify(''), 200


@data_blueprint.route('', methods=['DELETE'])
def delete_all_data() -> Tuple[Response, int]:
    """Remove all traces of all data associated with this session.
    :return: Empty response.
    """
    logger.debug("Received DELETE request on /data.")
    for task_id in session['data_tasks']:
        remove_data.delay(task_id)
    session['data_tasks'] = []
    logger.debug("Successfully removed all data from session. "
                 "Sending response.")
    return jsonify(''), 200

"""The /state controller."""

import re
import json
import logging
from uuid import UUID, uuid4
from typing import Tuple

from flask import Blueprint, jsonify, Response, request, session

from fractalis import redis
from fractalis.validator import validate_json, validate_schema
from fractalis.analytics.task import AnalyticTask
from fractalis.data.etlhandler import ETLHandler
from fractalis.data.controller import get_data_state_for_task_id
from fractalis.state.schema import request_state_access_schema


state_blueprint = Blueprint('state_blueprint', __name__)
logger = logging.getLogger(__name__)


@state_blueprint.route('', methods=['POST'])
@validate_json
def save_state() -> Tuple[Response, int]:
    """Save given payload to redis, so it can be accessed later on.
    :return: UUID linked to the saved state.
    """
    logger.debug("Received POST request on /state.")
    payload = request.get_json(force=True)
    # check if task ids in payload are valid
    matches = re.findall('\$.+?\$', str(payload))
    if not matches:
        error = "This state cannot be saved because it contains no data " \
                "task ids. These are used to verify access to the state and " \
                "its potentially sensitive data."
        logger.error(error)
        return jsonify({'error': error}), 400
    for match in matches:
        task_id, _ = AnalyticTask.parse_value(match)
        value = redis.get('data:{}'.format(task_id))
        if value is None:
            error = "Data task id is {} could not be found in redis. " \
                    "State cannot be saved".format(task_id)
            logger.error(error)
            return jsonify({'error': error}), 400
        try:
            json.loads(value)['meta']['descriptor']
        except (ValueError, KeyError):
            error = "Task with id {} was found in redis but it represents " \
                    "no valid data state. " \
                    "State cannot be saved.".format(task_id)
            return jsonify({'error': error}), 400
    uuid = uuid4()
    redis.set(name='state:{}'.format(uuid), value=json.dumps(payload))
    logger.debug("Successfully saved data to redis. Sending response.")
    return jsonify({'state_id': uuid}), 201


@state_blueprint.route('/<uuid:state_id>', methods=['POST'])
@validate_json
@validate_schema(request_state_access_schema)
def request_state_access(state_id: UUID) -> Tuple[Response, int]:
    """Traverse through the state object linked to the given UUID and look for
    data ids. Then attempt to load the data into the current session to verify
    access.
    :param state_id: The id associated with the saved state.
    :return: See redirect target.
    """
    logger.debug("Received POST request on /state/<uuid:state_id>.")
    wait = request.args.get('wait') == '1'
    payload = request.get_json(force=True)
    state_id = str(state_id)
    value = redis.get('state:{}'.format(state_id))
    if not value:
        error = "Could not find state associated with id {}".format(state_id)
        logger.error(error)
        return jsonify({'error': error}), 404
    descriptors = []
    matches = re.findall('\$.+?\$', str(json.loads(value)))
    for match in matches:
        task_id, _ = AnalyticTask.parse_value(match)
        value = redis.get('data:{}'.format(task_id))
        if value is None:
            error = "The state with id {} exists, but one or more of the " \
                    "associated data task ids are missing. Hence this saved " \
                    "state is lost forever because access can no longer be " \
                    "verified. Deleting state..."
            logger.error(error)
            redis.delete('state:{}'.format(state_id))
            return jsonify({'error': error}), 403
        data_state = json.loads(value)
        descriptors.append(data_state['meta']['descriptor'])
    etl_handler = ETLHandler.factory(handler=payload['handler'],
                                     server=payload['server'],
                                     auth=payload['auth'])
    task_ids = etl_handler.handle(descriptors=descriptors, wait=wait)
    session['data_tasks'] += task_ids
    session['data_tasks'] = list(set(session['data_tasks']))
    # if all task finish successfully we now that session has access to state
    session['state_access'][state_id] = task_ids
    logger.debug("Tasks successfully submitted. Sending response.")
    return jsonify(''), 202


@state_blueprint.route('/<uuid:state_id>', methods=['GET'])
def get_state_data(state_id: UUID) -> Tuple[Response, int]:
    """Check whether every ETL linked to the state_id successfully executed for
    this session. If and only if every ETL successfully completed grant access
    to the state information.
    :param state_id: ID of the state that is requested.
    :return: Previously saved state.
    """
    logger.debug("Received GET request on /state/<uuid:state_id>.")
    wait = request.args.get('wait') == '1'
    state_id = str(state_id)
    if state_id not in session['state_access']:
        error = "Cannot get state. Make sure to submit a POST request " \
                "to this very same URL containing credentials and server " \
                "data to launch access verification. Only after that a GET " \
                "request might or might not return you the saved state."
        logger.error(error)
        return jsonify({'error': error}), 404
    for task_id in session['state_access'][state_id]:
        data_state = get_data_state_for_task_id(task_id=task_id, wait=wait)
        if data_state['etl_state'] == 'SUBMITTED':
            return jsonify({'message': 'ETLs are still running.'}), 202
        elif data_state['etl_state'] == 'SUCCESS':
            continue
        else:
            error = "One or more ETLs failed or has unknown status. " \
                    "Assuming no access to saved state."
            logger.error(error)
            return jsonify({'error': error}), 403
    state = json.loads(redis.get('state:{}'.format(state_id)))
    return jsonify({'state': state}), 200

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
from fractalis.sync import remove_file


data_blueprint = Blueprint('data_blueprint', __name__)
logger = logging.getLogger(__name__)


def get_data_by_id(data_id: str, wait: bool) -> dict:
    """ Given a data id return the related Redis DB entry.
    :param data_id: The id computed based on the payload that was send to /data
    :param wait: Waits for celery to complete. Useful for testing or short jobs.
    :return: 
    """
    value = redis.get('data:{}'.format(data_id))
    if value is None:
        error = "Could not find data entry in Redis for data_id: " \
                "'{}'".format(data_id)
        logger.error(error, exc_info=1)
        raise LookupError(error)
    data_obj = json.loads(value.decode('utf-8'))
    job_id = data_obj['job_id']
    async_result = celery.AsyncResult(job_id)
    if wait:
        async_result.get(propagate=False)  # wait for results
    state = async_result.state
    result = async_result.result
    if isinstance(result, Exception):  # Exception -> str
        result = "{}: {}".format(type(result).__name__, str(result))
    data_obj['state'] = state
    data_obj['message'] = result
    data_obj['data_id'] = data_id
    # remove internal information from response
    del data_obj['file_path']
    del data_obj['access']
    return data_obj


@data_blueprint.before_request
def prepare_session() -> None:
    """Make sure the session is properly initialized before each request."""
    session.permanent = True
    if 'data_ids' not in session:
        session['data_ids'] = []


@data_blueprint.before_request
def cleanup_session() -> None:
    """Remove data_ids from session that have expired."""
    for data_id in session['data_ids']:
        logger.debug("Testing if data id '{}' has expired.".format(data_id))
        if not redis.exists('shadow:data:{}'.format(data_id)):
            logger.debug("Could not find shadow entry with id: '{}' in Redis. "
                         "Removing id from session.".format(data_id))
            session['data_ids'].remove(data_id)


@data_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_data_schema)
def create_data() -> Tuple[Response, int]:
    """Submit a new ETL task based on the payload of the request body.
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received POST request on /data.")
    wait = request.args.get('wait') == '1'
    payload = request.get_json(force=True)  # pattern enforced by decorators
    etl_handler = ETLHandler.factory(handler=payload['handler'],
                                     server=payload['server'],
                                     auth=payload['auth'])
    data_ids = etl_handler.handle(descriptors=payload['descriptors'],
                                  session_id=session.sid,
                                  wait=wait)
    session['data_ids'] += data_ids
    session['data_ids'] = list(set(session['data_ids']))  # make unique
    logger.debug("Job successfully submitted. Sending response.")
    return jsonify({'data_ids': data_ids}), 201


@data_blueprint.route('', methods=['GET'])
def get_all_data_state() -> Tuple[Response, int]:
    """Get information for all data within the current session.
    See doc/api/ for more information.
    :return: Flask Response 
    """
    logger.debug("Received GET request on /data.")
    wait = request.args.get('wait') == '1'
    data_states = [get_data_by_id(data_id, wait)
                   for data_id in session['data_ids']]
    logger.debug("Job successfully submitted. Sending response.")
    return jsonify({'data_states': data_states}), 200


@data_blueprint.route('/<string:params>', methods=['GET'])
def get_data_state(params) -> Tuple[Response, int]:
    """Get information for data that matches given arguments.
    :param params: Can be data ID or data descriptor
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received GET request on /data/params.")
    wait = request.args.get('wait') == '1'
    # params can be data_id or dict
    try:
        params = json.loads(params)
        data_id = ETLHandler.compute_data_id(server=params['server'],
                                             descriptor=params['descriptor'])
    except ValueError:
        logger.debug("Couldn't parse params: '{}'. "
                     "Assuming it is a data id.".format(params))
        data_id = params
    if data_id not in session['data_ids']:  # access control
        logger.error("Data ID '{}' not found in session. "
                     "Refusing access.".format(data_id))
        return jsonify(
            {'error_msg': "No matching data found. Maybe expired?"}), 404
    data_obj = get_data_by_id(data_id, wait)
    logger.debug("Successfully retrieved data state. Sending response.")
    return jsonify({'data_state': data_obj}), 200


@data_blueprint.route('/<string:params>', methods=['DELETE'])
def delete_data(params) -> Tuple[Response, int]:
    """Delete data from Redis, session, and FS for given params.
    :param params: Can be data ID or data descriptor
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received DELETE request on /data/params.")
    wait = request.args.get('wait') == '1'
    # params can be data_id or dict
    try:
        params = json.loads(params)
        data_id = ETLHandler.compute_data_id(server=params['server'],
                                             descriptor=params['descriptor'])
    except ValueError:
        logger.debug("Couldn't parse params: '{}'. "
                     "Assuming it is a data id.".format(params))
        data_id = params
    if data_id not in session['data_ids']:  # access control
        logger.error("Data ID '{}' not found in session. "
                     "Refusing access.".format(data_id))
        return jsonify(
            {'error_msg': "No matching data found. Maybe expired?"}), 404
    value = redis.get('data:{}'.format(data_id))
    data_obj = json.loads(value.decode('utf-8'))
    file_path = data_obj['file_path']
    async_result = remove_file.delay(file_path)
    if wait:
        async_result.get(propagate=False)
    redis.delete('data:{}'.format(data_id))
    redis.delete('shadow:data:{}'.format(data_id))
    session['data_ids'].remove(data_id)
    logger.debug("Successfully deleted data. Sending response.")
    return jsonify(''), 200


@data_blueprint.route('', methods=['DELETE'])
def delete_all_data() -> Tuple[Response, int]:
    """Call delete_data() for every data id in the current session.
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received DELETE request on /data.")
    for data_id in session['data_ids']:
        logging.debug("Using delete_data() for data id '{}'".format(data_id))
        delete_data(data_id)
    logger.debug("Successfully deleted all data. Sending response.")
    return jsonify(''), 200

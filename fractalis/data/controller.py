"""The /data controller. Please refer to doc/api for more information."""

import json
import time
import logging
from uuid import UUID
from typing import Tuple

from flask import Blueprint, session, request, jsonify
from flask.wrappers import Response

from fractalis.data.etlhandler import ETLHandler
from fractalis.data.schema import create_data_schema
from fractalis.validator import validate_json, validate_schema
from fractalis import celery, redis


data_blueprint = Blueprint('data_blueprint', __name__)
logger = logging.getLogger(__name__)


@data_blueprint.before_request
def prepare_session() -> None:
    """Make sure the session is properly initialized before each request."""
    session.permanent = True
    if 'jobs' not in session:
        logger.debug("Initializing jobs field in session dict.")
        session['jobs'] = []
    if 'data_ids' not in session:
        logger.debug("Initializing data_ids field in session dict.")
        session['data_ids'] = []


@data_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_data_schema)
def create_data_job() -> Tuple[Response, int]:
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
    job_ids = etl_handler.handle(descriptors=payload['descriptors'], wait=wait)
    session['jobs'] += job_ids
    session['jobs'] = list(set(session['data_jobs']))  # make unique
    logger.debug("Jobs successfully submitted. Sending response.")
    return jsonify({'job_ids': job_ids}), 201


@data_blueprint.route('/<uuid:job_id>', methods=['GET'])
def get_data_job_state(job_id: UUID) -> Tuple[Response, int]:
    """Get information for data that matches given job_id. If the job was
    successful add the data_id associated with the successful job to the session 
    for access control and return it. 
    :param job_id: The id associated with the previously submitted job.
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received GET request on /data/job_id.")
    job_id = str(job_id)
    wait = request.args.get('wait') == '1'
    if job_id not in session['jobs']:
        error = "Job ID '{}' not found in session. " \
                "Refusing access.".format(job_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    async_result = celery.AsyncResult(job_id)
    if wait:
        async_result.get(propagate=False)
    if async_result.state == 'SUCCESS':
        logger.debug("Job '{}' successful. Adding data_id '{}' "
                     "to session.".format(job_id, async_result.result))
        session['data_ids'] = async_result.result
    logger.debug("Job found and has access. Sending response.")
    return jsonify({'state': async_result.state,
                    'result': async_result.result}), 200


@data_blueprint.route('/<string:data_id>', methods=['GET'])
def get_data_by_id(data_id: str) -> Tuple[Response, int]:
    """Given a data id return the related Redis DB entry.
    :param data_id: The id returned by the data job submitted by create_data_job
    :return: Parsed and modified data entry from Redis.
    """
    logger.debug("Received GET request on /data/data_id.")
    if data_id not in session['data_ids']:
        error = "Data ID '{}' not found in session. " \
                "Refusing access.".format(data_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    value = redis.get('data:{}'.format(data_id))
    if not value:
        error = "Could not find data entry in Redis for data_id: " \
                "'{}'. The entry probably expired.".format(data_id)
        logger.warning(error)
        return jsonify({'error': error}), 404
    data_obj = json.loads(value.decode('utf-8'))
    # update 'last_access' internal
    data_obj['last_access'] = time.time()
    redis.set(name='data:{}'.format(data_id), value=data_obj)
    # remove internal information from response
    del data_obj['file_path']
    del data_obj['last_access']
    logger.debug("Data found and has access. Sending response.")
    return jsonify({'data_state': data_obj}), 200


@data_blueprint.route('', methods=['GET'])
def get_all_data_state() -> Tuple[Response, int]:
    """Get information for all data that the current session can access.
    See doc/api/ for more information.
    :return: Flask Response 
    """
    logger.debug("Received GET request on /data.")
    data_states = []
    for data_id in session['data_ids']:
        value = redis.get('data:{}'.format(data_id))
        if not value:
            error = "Could not find data entry in Redis for data_id: " \
                    "'{}'. The entry probably expired.".format(data_id)
            logger.warning(error)
            continue
        data_obj = json.loads(value.decode('utf-8'))
        # update 'last_access' internal
        data_obj['last_access'] = time.time()
        redis.set(name='data:{}'.format(data_id), value=data_obj)
        # remove internal information from response
        del data_obj['file_path']
        del data_obj['last_access']
        data_states.append(data_obj)
    logger.debug("Data states collected. Sending response.")
    return jsonify({'data_states': data_states}), 200


@data_blueprint.route('/<string:data_id>', methods=['DELETE'])
def delete_data(data_id: str) -> Tuple[Response, int]:
    """This only deletes data from the session, not Redis or the file system.
    This is enough to disable data visibility for the current user, but does not
    influence other users of the same data. Fractalis automatically removes
    entries that are no longer accessed after a certain period of time.
    :param data_id: The id returned by the data job submitted by create_data_job
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received DELETE request on /data/data_id.")
    if data_id in session['data_ids']:
        session['data_ids'].remove(data_id)
    logger.debug("Successfully removed data from session. Sending response.")
    return jsonify(''), 200


@data_blueprint.route('', methods=['DELETE'])
def delete_all_data() -> Tuple[Response, int]:
    """This only deletes data from the session, not Redis or the file system.
    This is enough to disable data visibility for the current user, but does not
    influence other users of the same data. Fractalis automatically removes
    entries that are no longer accessed after a certain period of time.
    See doc/api/ for more information.
    :return: Flask Response  
    """
    logger.debug("Received DELETE request on /data.")
    session['data_ids'] = []
    logger.debug("Successfully removed all data from session. "
                 "Sending response.")
    return jsonify(''), 200

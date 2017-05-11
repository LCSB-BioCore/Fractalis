"""The /analytics controller. Please refer to doc/api for more information."""

import logging
from typing import Tuple
from uuid import UUID

from flask import Blueprint, session, request, jsonify
from flask.wrappers import Response

from fractalis import celery
from fractalis.validator import validate_json, validate_schema
from fractalis.analytics.schema import create_job_schema
from fractalis.analytics.job import AnalyticsJob


analytics_blueprint = Blueprint('analytics_blueprint', __name__)
logger = logging.getLogger(__name__)


@analytics_blueprint.before_request
def prepare_session() -> None:
    """Make sure the session is properly initialized before each request."""
    session.permanent = True
    if 'jobs' not in session:
        logger.debug("Initializing jobs field in session dict.")
        session['jobs'] = []
    if 'data_ids' not in session:
        logger.debug("Initializing data_ids field in session dict.")
        session['data_ids'] = []


@analytics_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_job_schema)
def create_job() -> Tuple[Response, int]:
    """Create a new analytics job based on the parameters in the POST body.
    See doc/api/ for more information.
    :return: Flask Response 
    """
    logger.debug("Received POST request on /analytics.")
    json = request.get_json(force=True)  # pattern enforced by decorators
    analytics_job = AnalyticsJob.factory(json['job_name'])
    if analytics_job is None:
        logger.error("Could not submit job for unknown job name: "
                     "'{}'".format(json['job_name']))
        return jsonify({'error_msg': "Job with name '{}' not found.".format(
            json['job_name'])}), 400
    async_result = analytics_job.delay(accessible_data_ids=session['data_ids'],
                                       args=json['args'])
    session['jobs'].append(async_result.id)
    logger.debug("Job successfully submitted. Sending response.")
    return jsonify({'job_id': async_result.id}), 201


@analytics_blueprint.route('/<uuid:job_id>', methods=['GET'])
def get_job_details(job_id: UUID) -> Tuple[Response, int]:
    """Get job details for the given job_id.
     See doc/api/ for more information.
    :param job_id: ID returned on job creation.
    :return: Flask Response 
    """
    logger.debug("Received GET request on /analytics/job_id.")
    wait = request.args.get('wait') == '1'
    job_id = str(job_id)
    if job_id not in session['jobs']:
        error = "Job ID '{}' not found in session. " \
                "Refusing access.".format(job_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    async_result = celery.AsyncResult(job_id)
    if wait:
        async_result.get(propagate=False)  # make job synchronous
    logger.debug("Job found and has access. Sending response.")
    return jsonify({'state': async_result.state,
                    'result': async_result.result}), 200


@analytics_blueprint.route('/<uuid:job_id>', methods=['DELETE'])
def cancel_job(job_id: UUID) -> Tuple[Response, int]:
    """Cancel a job for a given job_id.
    See doc/api/ for more information.
    :param job_id: ID returned on job creation.
    :return: Flask Response
    """
    logger.debug("Received DELETE request on /analytics/job_id.")
    job_id = str(job_id)
    if job_id not in session['jobs']:
        error = "Job ID '{}' not found in session. " \
                "Refusing access.".format(job_id)
        logger.warning(error)
        return jsonify({'error': error}), 403
    wait = request.args.get('wait') == '1'
    # possibly dangerous: http://stackoverflow.com/a/29627549
    celery.control.revoke(job_id, terminate=True, signal='SIGUSR1', wait=wait)
    logger.debug("Successfully send term signal to task. Sending response.")
    return jsonify(''), 200

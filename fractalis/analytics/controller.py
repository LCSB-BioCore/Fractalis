from flask import Blueprint, session, request, jsonify

from fractalis import celery
from fractalis.validator import validate_json, validate_schema
from .schema import create_job_schema
from .job import AnalyticsJob


analytics_blueprint = Blueprint('analytics_blueprint', __name__)


@analytics_blueprint.before_request
def prepare_session():
    session.permanent = True
    if 'analytics_jobs' not in session:
        session['analytics_jobs'] = []
    if 'data_ids' not in session:
        session['data_ids'] = []


@analytics_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_job_schema)
def create_job():
    json = request.get_json(force=True)  # pattern enforced by decorators
    analytics_job = AnalyticsJob.factory(json['job_name'])
    if analytics_job is None:
        return jsonify({'error_msg': "Job with name '{}' not found.".format(
            json['job_name'])}), 400
    async_result = analytics_job.delay(session_id=session.sid,
                                       session_data_ids=session['data_ids'],
                                       args=json['args'])
    session['analytics_jobs'].append(async_result.id)
    return jsonify({'job_id': async_result.id}), 201


@analytics_blueprint.route('/<uuid:job_id>', methods=['GET'])
def get_job_details(job_id):
    wait = request.args.get('wait') == '1'
    job_id = str(job_id)
    if job_id not in session['analytics_jobs']:  # access control
        return jsonify({'error_msg': "No matching job found."}), 404
    async_result = celery.AsyncResult(job_id)
    if wait:
        async_result.get(propagate=False)  # make job synchronous
    state = async_result.state
    result = async_result.result
    if isinstance(result, Exception):  # Exception -> str
        result = "{}: {}".format(type(result).__name__, str(result))
    return jsonify({'state': state,
                    'result': result}), 200


@analytics_blueprint.route('/<uuid:job_id>', methods=['DELETE'])
def cancel_job(job_id):
    job_id = str(job_id)
    if job_id not in session['analytics_jobs']:  # Access control
        return jsonify({'error_msg': "No matching job found."}), 404
    wait = request.args.get('wait') == '1'
    # possibly dangerous: http://stackoverflow.com/a/29627549
    celery.control.revoke(job_id, terminate=True, signal='SIGUSR1', wait=wait)
    session['analytics_jobs'].remove(job_id)
    return jsonify({'job_id': job_id}), 200

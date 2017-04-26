import json

from flask import Blueprint, session, request, jsonify

from .etlhandler import ETLHandler
from .schema import create_data_schema
from fractalis.validator import validate_json, validate_schema
from fractalis import celery
from fractalis import redis


data_blueprint = Blueprint('data_blueprint', __name__)


@data_blueprint.before_request
def prepare_session():
    session.permanent = True
    if 'data_ids' not in session:
        session['data_ids'] = []


@data_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_data_schema)
def create_data():
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
    return jsonify({'data_ids': data_ids}), 201


def get_data_by_id(data_id, wait):
    value = redis.get('data:{}'.format(data_id))
    data_obj = json.loads(value)

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


@data_blueprint.route('/<string:params>', methods=['GET'])
def get_data_by_params(params):
    wait = request.args.get('wait') == '1'
    # params can be data_id or dict
    try:
        params = json.loads(params)
        data_id = ETLHandler.compute_data_id(server=params['server'],
                                             descriptor=params['descriptor'])
    except ValueError:
        data_id = params
    if data_id not in session['data_ids']:  # access control
        return jsonify(
            {'error_msg': "No matching data found. Maybe expired?"}), 404
    data_obj = get_data_by_id(data_id, wait)
    return jsonify({'data_state': data_obj}), 200


@data_blueprint.route('', methods=['GET'])
def get_all_data_state():
    wait = request.args.get('wait') == '1'
    data_states = [get_data_by_id(data_id, wait)
                   for data_id in session['data_ids']]
    return jsonify({'data_states': data_states}), 200

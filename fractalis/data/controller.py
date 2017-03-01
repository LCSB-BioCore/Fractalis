import json

from flask import Blueprint, session, request, jsonify

from .etls.etlhandler import ETLHandler
from .schema import create_data_schema
from fractalis.validator import validate_json, validate_schema
from fractalis.celery import app as celery
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
    json = request.get_json(force=True)  # pattern enforced by decorators
    etlhandler = ETLHandler.factory(handler=json['handler'],
                                    server=json['server'],
                                    token=json['token'])
    data_ids = etlhandler.handle(json['descriptors'])
    session['data_ids'] += data_ids
    return jsonify({'data_ids': data_ids}), 201


def get_data(key, wait):
    value = redis.hget(name='data', key=key)
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
    return data_obj


@data_blueprint.route('/<uuid:data_id>', methods=['GET'])
def get_data_by_id(data_id):
    data_id = str(data_id)
    wait = request.args.get('wait') == '1'
    if data_id not in session['data_ids']:  # access control
        return jsonify({'error_msg': "No matching data found."}), 404
    data = get_data(data_id, wait)
    return jsonify(data), 200


@data_blueprint.route('/<string:params>', methods=['GET'])
def get_data_by_params(params):
    params = json.loads(params)
    wait = request.args.get('wait') == '1'
    data_id = ETLHandler.compute_data_id(server=params['server'],
                                         descriptor=params['descriptor'])
    if data_id not in session['data_ids']:  # access control
        return jsonify({'error_msg': "No matching data found."}), 404
    data = get_data(data_id, wait)
    return jsonify(data), 200


@data_blueprint.route('', methods=['GET'])
def get_all_data_state():
    pass


@data_blueprint.route('/<uuid:data_id>', methods=['DELETE'])
def delete_data(data_id):
    pass


@data_blueprint.route('', methods=['DELETE'])
def delete_all_data():
    pass

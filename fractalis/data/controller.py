from flask import Blueprint, session, request, jsonify

from .etlhandler import ETLHandler
from .schema import create_data_schema
from fractalis.validator import validate_json, validate_schema


data_blueprint = Blueprint('data_blueprint', __name__)


@data_blueprint.before_request
def prepare_session():
    session.permanent = True
    if 'jobs' not in session:
        session['jobs'] = []


@data_blueprint.route('', methods=['POST'])
@validate_json
@validate_schema(create_data_schema)
def create_data():
    json = request.get_json(force=True)  # pattern enforced by decorators
    handler = ETLHandler.factory(json)
    job_ids = handler.handle()
    session['jobs'].append(job_ids)
    return jsonify({'job_ids': job_ids}), 201


@data_blueprint.route('', method=['GET'])
def get_all_session_data_status():
    pass


@data_blueprint.route('/<uuid:data_id>', method=['GET'])
def get_data_status(data_id):
    pass

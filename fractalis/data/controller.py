from flask import Blueprint, session, request, jsonify

from fractalis import redis
from fractalis.data.etls.etlhandler import ETLHandler


data_blueprint = Blueprint('data_blueprint', __name__)


@data_blueprint.route('', methods=['POST'])
def create_data(etl, server, concepts, token):
    ETLHandler.factory()
    return jsonify({data_status_list: data_status_list}), 201


@data_blueprint.route('', method=['GET'])
def get_all_session_data_status():
    pass


@data_blueprint.route('/<uuid:data_id>', method=['GET'])
def get_data_status(data_id):
    etls.get_status(data_id)

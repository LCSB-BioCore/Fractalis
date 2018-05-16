"""The /misc controller provides an API for everything that does not belong
in any of the other categories."""

import logging
from typing import Tuple

from flask import Blueprint, jsonify, Response


misc_blueprint = Blueprint('misc_blueprint', __name__)
logger = logging.getLogger(__name__)


@misc_blueprint.route('/version', methods=['GET'])
def get_version() -> Tuple[Response, int]:
    version = '0.5.4'
    return jsonify({'version': version}), 201

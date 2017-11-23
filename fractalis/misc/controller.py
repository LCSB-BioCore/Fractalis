"""The /misc controller provides an API for everything that does not belong
in any of the other categories."""

import logging
import pkg_resources
from typing import Tuple

from flask import Blueprint, jsonify, Response


misc_blueprint = Blueprint('misc_blueprint', __name__)
logger = logging.getLogger(__name__)


@misc_blueprint.route('/version', methods=['GET'])
def get_version() -> Tuple[Response, int]:
    version = pkg_resources.require('fractalis')[0].version
    return jsonify({'version': version}), 201

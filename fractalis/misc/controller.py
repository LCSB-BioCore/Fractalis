"""The /misc controller provides an API for everything that does not belong
in any of the other categories."""

import logging
from typing import Tuple

from flask import Blueprint, jsonify, Response

from fractalis.cleanup import janitor


misc_blueprint = Blueprint('misc_blueprint', __name__)
logger = logging.getLogger(__name__)


@misc_blueprint.route('/version', methods=['GET'])
def get_version() -> Tuple[Response, int]:
    version = '1.2.0'
    # this is a good place to launch the janitor because /version is one of the
    # first requests sent by the front-end on initialization
    janitor.delay()
    return jsonify({'version': version}), 201

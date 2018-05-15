"""The /misc controller provides an API for everything that does not belong
in any of the other categories."""

import logging
import re
from typing import Tuple

from flask import Blueprint, jsonify, Response


misc_blueprint = Blueprint('misc_blueprint', __name__)
logger = logging.getLogger(__name__)


@misc_blueprint.route('/version', methods=['GET'])
def get_version() -> Tuple[Response, int]:
    with open('setup.py') as setup_file:
        text = setup_file.read()
        version = re.search(r'version=\'(\d+\.\d+\.\d+)\',', text).group(1)
    return jsonify({'version': version}), 201

import functools
from enum import Enum
from functools import wraps
import requests
import jwt
from flask import request, jsonify
import logging
import json

from jwt.algorithms import RSAAlgorithm
from werkzeug.exceptions import Unauthorized

logger = logging.getLogger(__name__)
AuthServiceType = Enum('AuthServiceType', 'OIDC TRANSMART', module=__name__)


def authorized(f):
    @wraps(f)
    def _wrap(*args, **kwargs):
        try:
            payload = request.get_json(force=True)
            auth = payload.get('auth')
            token = auth.get('token') if len(auth) != 0 else ''
            if len(token) == 0:
                error = "No token in the authentication object."
                logger.error(error)
                raise Unauthorized(error)
            decoded_token = jwt.decode(token, verify=False)

            user = validate_user(decoded_token, auth)
            identity_provider_url = validate_identity_provider_url(decoded_token, auth)
            auth_service_type = auth.get('authServiceType', '').upper()
            if auth_service_type == AuthServiceType.OIDC.name:
                validate_token(token,  auth.get('oidcClientId'), identity_provider_url)
            else:
                logger.warning(f'Token not validated. Validation not supported for "{auth_service_type}" service type')
        except Exception as e:
            return jsonify({'error': f'Access unauthorized. {str(e)}'}), 403
        logger.info(f'Connected: {decoded_token.get("email")!r}, user id (sub): {user!r}')
        return f(*args, **kwargs)
    return _wrap


def validate_identity_provider_url(decoded_token, auth) -> str:
    """ Checks if the token issuer (iss) matches the identity provider url from authentication object
    :param decoded_token: decoded user token
    :param auth: authentication object from the request arguments
    :return: identity_provider_url or
             Unauthorized if urls do not match
    """
    token_issuer = f'{decoded_token.get("iss")}/protocol/openid-connect'
    identity_provider_url = auth.get('oidcServerUrl')
    if token_issuer != identity_provider_url:
        error = "Token issuer does not match the identity provider url from authentication object parameter."
        logger.error(error)
        raise Unauthorized(error)
    return identity_provider_url


def validate_user(decoded_token, auth):
    """ Checks if the current user (in the token) matches the user from authentication object
    :param decoded_token: decoded user token
    :param auth: authentication object from the request arguments
    :return: user or
             Unauthorized if users do not match
    """
    subject = decoded_token.get('sub')
    user = auth.get('user')
    if user != subject:
        error = "Token user does not match the user from authentication object parameter."
        logger.error(error)
        raise Unauthorized(error)
    return user


def validate_token(token: str, client_id: str, oidc_server_url: str):
    """ Validate the request token with public key
    :param token: the user token
    :param client_id: id of the oidc client
    :param oidc_server_url: url of the server to authorize with
    :return: Unauthorized if token is invalid
    """
    try:
        logger.info(f'Validating the token...')
        decoded_token_header = jwt.get_unverified_header(token)
        token_kid = decoded_token_header.get('kid')
        algorithm, public_key = retrieve_keycloak_public_key_and_algorithm(token_kid, oidc_server_url)
        jwt.decode(token, public_key, algorithms=algorithm, audience=client_id)
    except Exception as e:
        error = f"Token validation failed. Reason: {e}."
        logger.error(error)
        raise Unauthorized(error)


@functools.lru_cache(maxsize=2)
def retrieve_keycloak_public_key_and_algorithm(token_kid: str, oidc_server_url: str) -> (str, str):
    """ Retrieve the public key for the token from keycloak
    :param token_kid: The user token
    :param oidc_server_url:  Url of the server to authorize with
    :return: keycloak public key and algorithm
    """
    handle = f'{oidc_server_url}/certs'
    logger.info(f'Getting public key for the kid={token_kid} from the keycloak...')
    r = requests.get(handle)
    if r.status_code != 200:
        error = "Could not get certificates from the keycloak. " \
                "Reason: [{}]: {}".format(r.status_code, r.text)
        logger.error(error)
        raise ValueError(error)
    try:
        json_response = r.json()
    except Exception:
        error = "Could not retrieve the public key. " \
                 "Got unexpected response: '{}'".format(r.text)
        logger.error(error)
        raise ValueError(error)
    try:
        matching_key = next((item for item in json_response.get('keys') if item['kid'] == token_kid), None)
        matching_key_json = json.dumps(matching_key)
        public_key = RSAAlgorithm.from_jwk(matching_key_json)
    except Exception as e:
        error = f'Invalid public key!. Reason: {e}'
        logger.error(error)
        raise ValueError(error)
    logger.info(f'The public key for the kid={token_kid} has been fetched.')
    return matching_key.get('alg'), public_key

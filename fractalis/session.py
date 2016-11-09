import math
import datetime
from uuid import uuid4

from redis import StrictRedis
from flask.sessions import SecureCookieSessionInterface, SecureCookieSession


class RedisSession(SecureCookieSession):
    """An implementation of SecureCookieSession that expands the class with
    a sid field."""

    def __init__(self, sid, initial=None):
        super().__init__(initial=initial)
        self.sid = sid


class RedisSessionInterface(SecureCookieSessionInterface):
    """An implementation of SecureCookieSessionInterface that makes use of
    Redis as a session storage.

    Fields:
    redis (StrictRedis) -- The connection to the Redis database
    sid (UUID) -- A session id
    """

    def __init__(self, app_config):
        self.redis = StrictRedis(host=app_config['REDIS_HOSTNAME'],
                                 port=app_config['REDIS_PORT'])

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if not sid:
            sid = str(uuid4())
            return RedisSession(sid=sid)
        session_data = self.redis.get('session:{}'.format(sid))
        if session_data is not None:
            session_data = self.serializer.loads(session_data)
            return RedisSession(initial=session_data, sid=sid)
        return RedisSession(sid=sid)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if not session:
            self.redis.delete('session:{}'.format(session.sid))
            if session.modified:
                response.delete_cookie(app.session_cookie_name, domain=domain)
            return None
        expiration_times = self.get_expiration_times(app, session)
        serialzed_session_data = self.serializer.dumps(dict(session))
        self.redis.setex(name='session:{}'.format(session.sid),
                         time=expiration_times['redis'],
                         value=serialzed_session_data)
        response.set_cookie(key=app.session_cookie_name, value=session.sid,
                            expires=expiration_times['cookies'], httponly=True,
                            domain=domain)

    def get_expiration_times(self, app, session):
        """Get dictionary that contains redis session and cookie expiration
        times in the correct format.

        We need this method for two reasons. First, if the expiration time is
        None we need to set it to a default. Second, there is a bug that
        prohibits redislite.Redis.setex method to use a datetime object for
        expiration time, so this method converts it to integer (seconds).

        Keyword Arguments:
        app (Flask) -- An instance of a Flask application
        session (SecureCookieSession) -- An instance of a session

        Returns:
        (dict) -- A dict containing expiration times for redis and cookie
        """
        expiration_times = {'redis': 60 * 60 * 24, 'cookies': None}
        now = datetime.datetime.utcnow()
        session_expiration_time = self.get_expiration_time(app, session)
        if session_expiration_time is not None:
            seconds = (session_expiration_time - now).total_seconds()
            expiration_times['redis'] = math.ceil(seconds)
        cookie_expiration_time = (now + datetime.timedelta(
            seconds=expiration_times['redis']))
        expiration_times['cookies'] = cookie_expiration_time
        return expiration_times

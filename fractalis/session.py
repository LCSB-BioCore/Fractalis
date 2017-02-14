from uuid import uuid4

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

    def __init__(self, redis):
        self.redis = redis

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
        session_expiration_time = app.config['PERMANENT_SESSION_LIFETIME']
        cookie_expiration_time = self.get_expiration_time(app, session)
        serialzed_session_data = self.serializer.dumps(dict(session))
        self.redis.setex(name='session:{}'.format(session.sid),
                         time=session_expiration_time,
                         value=serialzed_session_data)
        response.set_cookie(key=app.session_cookie_name, value=session.sid,
                            expires=cookie_expiration_time, httponly=True,
                            domain=domain)

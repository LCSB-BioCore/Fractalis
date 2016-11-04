from uuid import uuid4

from redislite import StrictRedis

from fractalis import flask_app
from flask.sessions import SecureCookieSessionInterface, SecureCookieSession


class RedisSession(SecureCookieSession):

    def __init__(self, sid, initial=None):
        super().__init__(initial=initial)
        self.sid = sid


class RedisSessionInterface(SecureCookieSessionInterface):

    def __init__(self):
        self.redis = StrictRedis(flask_app.config['REDIS_DB_PATH'])

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
        session_expiration_time = self.get_expiration_time(app, session)
        serialzed_session_data = self.serializer.dumps(dict(session))
        self.redis.setex('session:{}'.format(session.sid),
                         session_expiration_time, serialzed_session_data)
        response.set_cookie(app.session_cookie_name, session.sid,
                            expires=session_expiration_time, httponly=True,
                            domain=domain)

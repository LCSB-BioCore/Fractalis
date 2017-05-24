import json
from uuid import uuid4
from time import sleep

from werkzeug.datastructures import CallbackDict
from flask.sessions import SessionMixin, SessionInterface


class RedisSession(CallbackDict, SessionMixin):

    def __init__(self, sid, initial=None):
        if initial is None:
            initial = {'data_tasks': [], 'analytic_tasks': [], 'subsets': []}

        def on_update(self):
            self.modified = True
        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        self.permanent = True
        self.modified = False

class RedisSessionInterface(SessionInterface):

    def __init__(self, redis):
        self.redis = redis

    def acquire_lock(self, sid, request_id):
        if self.redis.get(name='session:{}:lock'.format(sid)) == request_id:
            return
        while self.redis.getset(name='session:{}:lock'.format(sid),
                                value=request_id):
            sleep(0.1)
        self.redis.setex(name='session:{}:lock'.format(sid),
                         value=request_id, time=10)

    def release_lock(self, sid):
        self.redis.delete('session:{}:lock'.format(sid))

    def open_session(self, app, request):
        request_id = request.environ.get("FLASK_REQUEST_ID")
        sid = request.cookies.get(app.session_cookie_name)
        if not sid:
            sid = str(uuid4())
            self.acquire_lock(sid, request_id)
            return RedisSession(sid=sid)
        self.acquire_lock(sid, request_id)
        session_data = self.redis.get('session:{}'.format(sid))
        if session_data is not None:
            session_data = json.loads(session_data)
            return RedisSession(sid=sid, initial=session_data)
        return RedisSession(sid=sid)

    def save_session(self, app, session, response):
        path = self.get_cookie_path(app)
        domain = self.get_cookie_domain(app)
        if not session:
            if session.modified:
                self.redis.delete('session:{}'.format(session.sid))
                response.delete_cookie(app.session_cookie_name,
                                       domain=domain, path=path)
            self.release_lock(session.sid)
            return
        session_expiration_time = app.config['PERMANENT_SESSION_LIFETIME']
        cookie_expiration_time = self.get_expiration_time(app, session)
        serialzed_session_data = json.dumps(dict(session))
        self.redis.setex(name='session:{}'.format(session.sid),
                         time=session_expiration_time,
                         value=serialzed_session_data)
        self.release_lock(session.sid)
        response.set_cookie(key=app.session_cookie_name, value=session.sid,
                            expires=cookie_expiration_time, httponly=True,
                            domain=domain)


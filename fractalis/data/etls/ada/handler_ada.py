import requests

from fractalis.data.etlhandler import ETLHandler


class AdaHandler(ETLHandler):

    _handler = 'ada'

    def _heartbeat(self):
        pass

    def _get_token_for_credentials(self, server: str,
                                   user: str, passwd: str) -> str:
        r = requests.post(url='{}/login'.format(server),
                          headers={'Accept': 'application/json'},
                          data={'id': user, 'password': passwd})
        assert r.status_code == 200
        cookie = r.headers['Set-Cookie']
        token = [s for s in cookie.split(';')
                 if s.startswith('PLAY2AUTH_SESS_ID')][0]
        return token

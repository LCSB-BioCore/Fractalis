from fractalis.data.etlhandler import ETLHandler


class AdaHandler(ETLHandler):

    _handler = 'ada'

    def _heartbeat(self):
        pass

    def _get_token_for_credentials(self, server: str,
                                   user: str, passwd: str) -> str:
        pass
from fractalis.data.etlhandler import ETLHandler


class TestHandler(ETLHandler):

    _HANDLER = 'test'

    def _heartbeat():
        pass

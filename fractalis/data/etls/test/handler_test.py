from fractalis.data.etlhandler import ETLHandler


class TestHandler(ETLHandler):

    _handler = 'test'

    def _heartbeat():
        pass

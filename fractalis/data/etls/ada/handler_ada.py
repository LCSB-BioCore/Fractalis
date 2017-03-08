from fractalis.data.etlhandler import ETLHandler


class AdaHandler(ETLHandler):

    _handler = 'ada'

    def _heartbeat(self):
        pass
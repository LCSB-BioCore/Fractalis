from fractalis.data.etlhandler import ETLHandler


class AdaHandler(ETLHandler):

    _HANDLER = 'ada'

    def _heartbeat(self):
        pass
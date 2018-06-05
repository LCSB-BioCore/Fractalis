from fractalis.data.etlhandler import ETLHandler


class DemoHandler(ETLHandler):

    _handler = 'demo_wine_quality'

    @staticmethod
    def make_label(descriptor):
        return descriptor.get('field')

    def _get_token_for_credentials(self, server: str, auth: dict) -> str:
        return 'foo'

    def _heartbeat(self):
        pass

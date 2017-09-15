from fractalis.data.etlhandler import ETLHandler


class TestHandler(ETLHandler):

    _handler = 'test'

    @staticmethod
    def make_label(descriptor):
        return descriptor.get('label')

    def _get_token_for_credentials(self, server: str, auth: dict) -> str:
        return 'abc'

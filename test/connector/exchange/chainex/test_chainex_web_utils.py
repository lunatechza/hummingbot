from unittest import TestCase

from hummingbot.connector.exchange.chainex import chainex_constants as CONSTANTS, chainex_web_utils as web_utils


class WebUtilsTests(TestCase):
    def test_rest_url(self):
        url = web_utils.rest_url(path_url=CONSTANTS.LAST_TRADED_PRICE_PATH, domain=CONSTANTS.REST_URL)
        self.assertEqual('https://api.chainex.com/spot/quote/v1/ticker/price', url)
        url = web_utils.rest_url(path_url=CONSTANTS.LAST_TRADED_PRICE_PATH, domain='chainex_testnet')
        self.assertEqual('https://api-testnet.chainex.com/spot/quote/v1/ticker/price', url)

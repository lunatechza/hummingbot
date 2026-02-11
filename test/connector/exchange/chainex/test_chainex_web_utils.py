from unittest import TestCase

from hummingbot.connector.exchange.chainex import chainex_constants as CONSTANTS, chainex_web_utils as web_utils


class WebUtilsTests(TestCase):
    def test_rest_url(self):
        self.assertEqual(
            "https://api.chainex.io/stats/",
            web_utils.rest_url(path_url=CONSTANTS.TICKER_BOOK_PATH_URL, domain=CONSTANTS.DEFAULT_DOMAIN),
        )
        self.assertEqual(
            "https://api.chainex.test/stats/",
            web_utils.rest_url(path_url=CONSTANTS.TICKER_BOOK_PATH_URL, domain=CONSTANTS.TESTNET_DOMAIN),
        )

    def test_ws_urls(self):
        self.assertEqual(
            "wss://push.chainex.io/ws/market",
            web_utils.public_ws_url(domain=CONSTANTS.DEFAULT_DOMAIN),
        )
        self.assertEqual(
            "wss://push.chainex.test/ws/account",
            web_utils.private_ws_url(domain=CONSTANTS.TESTNET_DOMAIN),
        )

import asyncio
import json
import re
import unittest
from typing import Awaitable, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.chainex import chainex_constants as CONSTANTS, chainex_web_utils as web_utils
from hummingbot.connector.exchange.chainex.chainex_api_order_book_data_source import ChainEXAPIOrderBookDataSource
from hummingbot.connector.exchange.chainex.chainex_exchange import ChainEXExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book_message import OrderBookMessage


class TestChainEXAPIOrderBookDataSource(unittest.TestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.domain = CONSTANTS.REST_URL

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []
        self.async_task = None
        self.mocking_assistant = NetworkMockingAssistant()

        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.connector = ChainEXExchange(
            client_config_map=client_config_map,
            chainex_api_key="",
            chainex_api_secret="",
            trading_pairs=[self.trading_pair])

        self.throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        self.ob_data_source = ChainEXAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            throttler=self.throttler,
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory)

        self._original_full_order_book_reset_time = self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS
        self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = -1

        self.ob_data_source.logger().setLevel(1)
        self.ob_data_source.logger().addHandler(self)

        self.resume_test_event = asyncio.Event()

        self.connector._set_trading_pair_symbol_map(bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.async_task and self.async_task.cancel()
        self.ob_data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = self._original_full_order_book_reset_time
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    def _create_exception_and_unlock_test_with_event(self, exception):
        self.resume_test_event.set()
        raise exception

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @aioresponses()
    def test_fetch_trading_pairs(self, mock_api):
        ChainEXAPIOrderBookDataSource._trading_pair_symbol_map = {}
        url = web_utils.public_rest_url(path_url=CONSTANTS.MARKET_SUMMARY_PATH_URL, domain=self.domain)

        mock_response: Dict[str, Any] = {
            "status": "success",
            "count": "1",
            "data": [
                {
                    "market": "BTC/USDT",
                    "coin_decimals": "8",
                    "exchange_decimals": "2",
                    "yesterday_price": "20775.81000000",
                    "last_price": "20775.81000000",
                    "volume_amount": "0.00",
                    "last_trade_time": "1673695300.7261",
                    "change": "0.00",
                    "top_bid": "20781.60000000",
                    "top_ask": "20963.12000000",
                    "spread_price": 20872.36,
                    "24hhigh": "0.00000000",
                    "24hlow": "0.00000000",
                    "24hvol": "0.00",
                    "code": "BTC",
                    "name": "Bitcoin",
                    "exchange": "USDT"
                    },
            ]
        }

        mock_api.get(url, body=json.dumps(mock_response))

        result: Dict[str] = self.async_run_with_timeout(
            self.data_source.fetch_trading_pairs()
        )

        self.assertEqual(0, len(result))
        self.assertIn("BTC/USDT", result)
        self.assertNotIn("BTC/USDT", result)

    def _snapshot_response(self,
                           update_id=1027024):
        resp = {
            "data": [{
                "top_bid": [
                    [
                        "5175.07000000"
                    ]
                ],
                "top_ask": [
                    [
                        "5226.82000000"
                    ]
                ],
                "market": self.ex_trading_pair,
            }]
        }
        return resp

    @aioresponses()
    def test_fetch_trading_pairs_exception_raised(self, mock_api, retry_sleep_time_mock):
        retry_sleep_time_mock.side_effect = lambda *args, **kwargs: 0
        ChainEXAPIOrderBookDataSource._trading_pair_symbol_map = {}

        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL, return_url=True)

        mock_api.get(regex_url, exception=Exception)

        result: Dict[str] = self.async_run_with_timeout(
            self.data_source.fetch_trading_pairs()
        )

        self.assertEqual(0, len(result))

    @aioresponses()
    def test_get_last_trade_prices(self, mock_api):
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL, return_url=True)

        mock_response: Dict[str, Any] = {
            "status": "success",
            "count": "1",
            "data": [
                {
                    "market": "BTC/USDT",
                    "coin_decimals": "8",
                    "exchange_decimals": "2",
                    "yesterday_price": "20775.81000000",
                    "last_price": "20775.81000000",
                    "volume_amount": "0.00",
                    "last_trade_time": "1673695300.7261",
                    "change": "0.00",
                    "top_bid": "20781.60000000",
                    "top_ask": "20963.12000000",
                    "spread_price": 20872.36,
                    "24hhigh": "0.00000000",
                    "24hlow": "0.00000000",
                    "24hvol": "0.00",
                    "code": "BTC",
                    "name": "Bitcoin",
                    "exchange": "USDT"
                    },
            ]
        }


        mock_api.get(regex_url, body=json.dumps(mock_response))

        result: Dict[str, float] = self.async_run_with_timeout(
            self.data_source.get_last_traded_prices(trading_pairs=[self.trading_pair],
                                                    throttler=self.throttler)
        )

        self.assertEqual(1, len(result))
        self.assertEqual(100, result[self.trading_pair])

    @aioresponses()
    def test_get_last_trade_prices_exception_raised(self, mock_api):
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL, return_url=True)

        mock_api.get(regex_url, body=json.dumps([{"marketCode": "COINALPHA-HBOT"}]))

        with self.assertRaises(IOError):
            self.async_run_with_timeout(
                self.data_source.get_last_traded_prices(trading_pairs=[self.trading_pair],
                                                        throttler=self.throttler)
            )

    @aioresponses()
    def test_get_snapshot_successful(self, mock_api):
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, body=json.dumps(self._snapshot_response()))

        result: Dict[str, Any] = self.async_run_with_timeout(
            self.data_source.get_snapshot(self.trading_pair)
        )

        self.assertEqual(self._snapshot_response()["data"][0], result)

    @aioresponses()
    @patch("hummingbot.connector.exchange.coinflex.coinflex_web_utils.retry_sleep_time")
    def test_get_snapshot_catch_exception(self, mock_api, retry_sleep_time_mock):
        retry_sleep_time_mock.side_effect = lambda *args, **kwargs: 0
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, status=400)
        with self.assertRaises(IOError):
            self.async_run_with_timeout(
                self.data_source.get_snapshot(self.trading_pair)
            )

        mock_api.get(regex_url, body=json.dumps({}))
        with self.assertRaises(IOError):
            self.async_run_with_timeout(
                self.data_source.get_snapshot(self.trading_pair)
            )

    @aioresponses()
    def test_get_new_order_book(self, mock_api):
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, body=json.dumps(self._snapshot_response(update_id=1)))

        result: OrderBook = self.async_run_with_timeout(
            self.data_source.get_new_order_book(self.trading_pair)
        )

        self.assertEqual(1, result.snapshot_uid)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_subscribes_to_trades_and_order_diffs(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self._login_message()))

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(1, len(sent_subscription_messages))
        expected_subscription = {
            "op": "subscribe",
            "args": [
                f"trade:{self.ex_trading_pair}",
                f"depth:{self.ex_trading_pair}",
            ],
        }
        self.assertEqual(expected_subscription, sent_subscription_messages[0])

        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to public order book and trade channels..."
        ))

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _: AsyncMock):
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())
            self.async_run_with_timeout(self.listening_task)

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds..."))

    def test_listen_for_trades_cancelled_when_listening(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[CONSTANTS.MARKET_SUMMARY_PATH_URL] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_trades_logs_exception(self):
        incomplete_resp = {
            "data": [{
                "m": 1,
                "i": 2,
            }],
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[CONSTANTS.MARKET_SUMMARY_PATH_URL] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_trades(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public trade updates from exchange"))

    def test_listen_for_trades_successful(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [self._login_message(), self._trade_update_event(), asyncio.CancelledError()]
        self.data_source._message_queue[CONSTANTS.MARKET_SUMMARY_PATH_URL] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_trades(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(-1, msg.update_id)

    def test_listen_for_order_book_diffs_cancelled(self):
        mock_queue = AsyncMock()
        mock_queue.get.side_effect = asyncio.CancelledError()
        self.data_source._message_queue[CONSTANTS.MARKET_SUMMARY_PATH_URL] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
            self.async_run_with_timeout(self.listening_task)

    def test_listen_for_order_book_diffs_logs_exception(self):
        incomplete_resp = {
            "data": [{
                "m": 1,
                "i": 2,
            }],
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[CONSTANTS.MARKET_SUMMARY_PATH_URL] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
        )

        try:
            self.async_run_with_timeout(self.listening_task)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error when processing public order book updates from exchange"))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    def test_listen_for_order_book_diffs_successful(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self._login_message()))

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(self._order_diff_event()))

        self.listening_task = self.ev_loop.create_task(self.data_source.listen_for_subscriptions())

        self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [self._login_message(), self._order_diff_event(), asyncio.CancelledError()]
        self.data_source._message_queue[CONSTANTS.MARKET_SUMMARY_PATH_URL] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            self.listening_task = self.ev_loop.create_task(
                self.data_source.listen_for_order_book_diffs(self.ev_loop, msg_queue)
            )
        except asyncio.CancelledError:
            pass

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(123456789, msg.update_id)

    @aioresponses()
    def test_listen_for_order_book_snapshots_cancelled_when_fetching_snapshot(self, mock_api):
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, exception=asyncio.CancelledError)

        with self.assertRaises(asyncio.CancelledError):
            self.async_run_with_timeout(
                self.data_source.listen_for_order_book_snapshots(self.ev_loop, asyncio.Queue())
            )

    @aioresponses()
    @patch("hummingbot.connector.exchange.coinflex.coinflex_api_order_book_data_source"
           ".CoinflexAPIOrderBookDataSource._sleep")
    @patch("hummingbot.connector.exchange.coinflex.coinflex_web_utils.retry_sleep_time")
    def test_listen_for_order_book_snapshots_log_exception(self, mock_api, retry_sleep_time_mock, sleep_mock):
        retry_sleep_time_mock.side_effect = lambda *args, **kwargs: 0
        msg_queue: asyncio.Queue = asyncio.Queue()
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(asyncio.CancelledError())

        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, exception=Exception)

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )
        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged("ERROR", f"Unexpected error fetching order book snapshot for {self.trading_pair}."))

    @aioresponses()
    @patch("hummingbot.connector.exchange.coinflex.coinflex_api_order_book_data_source"
           ".CoinflexAPIOrderBookDataSource._sleep")
    def test_listen_for_order_book_snapshots_log_outer_exception(self, mock_api, sleep_mock):
        msg_queue: asyncio.Queue = asyncio.Queue()
        sleep_mock.side_effect = lambda _: self._create_exception_and_unlock_test_with_event(Exception("Dummy"))

        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, body=json.dumps(self._snapshot_response()))

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )
        self.async_run_with_timeout(self.resume_test_event.wait())

        self.assertTrue(
            self._is_logged("ERROR", "Unexpected error."))

    @aioresponses()
    def test_listen_for_order_book_snapshots_successful(self, mock_api, ):
        msg_queue: asyncio.Queue = asyncio.Queue()
        url, regex_url = self._get_regex_url(CONSTANTS.MARKET_SUMMARY_PATH_URL.format(self.trading_pair, 1000), return_url=True)

        mock_api.get(regex_url, body=json.dumps(self._snapshot_response()))

        self.listening_task = self.ev_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.ev_loop, msg_queue)
        )

        msg: OrderBookMessage = self.async_run_with_timeout(msg_queue.get())

        self.assertEqual(1027024, msg.update_id)
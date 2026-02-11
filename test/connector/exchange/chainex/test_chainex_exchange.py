import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.chainex import chainex_constants as CONSTANTS
from hummingbot.connector.exchange.chainex.chainex_exchange import ChainEXExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


class TestChainEXExchange(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()

    def setUp(self) -> None:
        super().setUp()
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.exchange = ChainEXExchange(
            self.client_config_map,
            "someKey",
            "someSecretKey",
            trading_pairs=["COINALPHA-USDT"],
        )

    def async_run_with_timeout(self, coroutine, timeout: int = 1):
        return self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))

    def test_name_for_default_and_testnet_domains(self):
        self.assertEqual("chainex", self.exchange.name)
        testnet = ChainEXExchange(self.client_config_map, "k", "s", domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual("chainex_testnet", testnet.name)

    def test_supported_order_types(self):
        self.assertEqual([OrderType.LIMIT, OrderType.LIMIT_MAKER], self.exchange.supported_order_types())

    def test_order_state_from_exchange(self):
        self.assertEqual(OrderState.FILLED, self.exchange._order_state_from_exchange("filled"))
        self.assertEqual(OrderState.CANCELED, self.exchange._order_state_from_exchange("cancelled"))
        self.assertEqual(OrderState.OPEN, self.exchange._order_state_from_exchange("unknown_status"))

    def test_place_cancel_uses_exchange_order_id_attribute(self):
        tracked_order = InFlightOrder(
            client_order_id="OID1",
            exchange_order_id="1001",
            trading_pair="COINALPHA-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1,
            price=Decimal("1"),
        )
        self.exchange._api_delete = AsyncMock(return_value={"status": "success"})
        result = self.async_run_with_timeout(self.exchange._place_cancel("OID1", tracked_order))
        self.assertTrue(result)
        self.exchange._api_delete.assert_awaited_once()

    def test_update_orders_fills_processes_trade_updates(self):
        tracked_order = InFlightOrder(
            client_order_id="OID1",
            exchange_order_id="1001",
            trading_pair="COINALPHA-USDT",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1,
            price=Decimal("1"),
        )
        trade_update = AsyncMock()

        self.exchange._all_trade_updates_for_order = AsyncMock(return_value=[trade_update])
        self.exchange._order_tracker.process_trade_update = AsyncMock()

        self.async_run_with_timeout(self.exchange._update_orders_fills([tracked_order]))

        self.exchange._all_trade_updates_for_order.assert_awaited_once_with(order=tracked_order)
        self.exchange._order_tracker.process_trade_update.assert_called_once_with(trade_update=trade_update)

import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.chainex.chainex_api_order_book_data_source import ChainEXAPIOrderBookDataSource
from hummingbot.connector.exchange.chainex.chainex_exchange import ChainEXExchange
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter


class ChainEXAPIOrderBookDataSourceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.exchange = ChainEXExchange(client_config_map, "k", "s", trading_pairs=["BTC-USDT"])
        self.data_source = ChainEXAPIOrderBookDataSource(
            trading_pairs=["BTC-USDT"],
            connector=self.exchange,
            api_factory=self.exchange._web_assistants_factory,
            throttler=self.exchange._throttler,
        )

    async def test_listen_for_order_book_snapshots_enqueues_messages(self):
        self.data_source.get_snapshot = AsyncMock(
            return_value={
                "data": [
                    {"type": "buy", "orders": [{"price": "10", "amount": "1"}]},
                    {"type": "sell", "orders": [{"price": "11", "amount": "1"}]},
                ]
            }
        )
        output = asyncio.Queue()

        task = asyncio.create_task(self.data_source.listen_for_order_book_snapshots(asyncio.get_running_loop(), output))
        await asyncio.sleep(0.1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertGreaterEqual(output.qsize(), 1)

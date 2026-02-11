from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.chainex.chainex_api_user_stream_data_source import ChainEXAPIUserStreamDataSource
from hummingbot.connector.exchange.chainex.chainex_exchange import ChainEXExchange


class ChainEXAPIUserStreamDataSourceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        client_config_map = ClientConfigAdapter(ClientConfigMap())
        self.exchange = ChainEXExchange(client_config_map, "k", "s", trading_pairs=["BTC-USDT"])
        self.data_source = ChainEXAPIUserStreamDataSource(
            auth=self.exchange.authenticator,
            trading_pairs=["BTC-USDT"],
            connector=self.exchange,
            api_factory=self.exchange._web_assistants_factory,
            time_synchronizer=self.exchange._time_synchronizer,
        )

    async def test_subscribe_channels_sends_payload(self):
        ws = AsyncMock()
        await self.data_source._subscribe_channels(ws)
        ws.send.assert_awaited_once()

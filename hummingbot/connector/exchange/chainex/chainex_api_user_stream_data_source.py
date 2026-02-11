import asyncio
import logging
import time
from typing import TYPE_CHECKING, List, Optional

import hummingbot.connector.exchange.chainex.chainex_constants as CONSTANTS
import hummingbot.connector.exchange.chainex.chainex_web_utils as web_utils
from hummingbot.connector.exchange.chainex.chainex_auth import ChainEXAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.chainex.chainex_exchange import ChainEXExchange


class ChainEXAPIUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0

    _bausds_logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: ChainEXAuth,
                 trading_pairs: List[str],
                 connector: "ChainEXExchange",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._ws_assistants: List[WSAssistant] = []
        self._time_synchronizer = time_synchronizer or TimeSynchronizer()
        self._ws_assistant: Optional[WSAssistant] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    @property
    def last_recv_time(self) -> float:
        t = 0.0
        if len(self._ws_assistants) > 0:
            t = min([wsa.last_recv_time for wsa in self._ws_assistants])
        return t

    async def listen_for_user_stream(self, output: asyncio.Queue):
        tasks_future = None
        try:
            timestamp = int(self._time_synchronizer.time() * 1e3)
            url = self._auth.add_auth_to_url(web_utils.private_ws_url(domain=self._domain), timestamp=timestamp)
            headers = self._auth.get_auth_headers(web_utils.private_ws_url(domain=self._domain), timestamp=timestamp)
            tasks_future = asyncio.gather(self._listen_for_user_stream_on_url(url=url, headers=headers, output=output))
            await tasks_future
        except asyncio.CancelledError:
            if tasks_future is not None:
                tasks_future.cancel()
            raise

    async def _listen_for_user_stream_on_url(self, url: str, headers: dict, output: asyncio.Queue):
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws = await self._connected_websocket_assistant(url, headers)
                self._ws_assistants.append(ws)
                await self._subscribe_channels(ws)
                await self._process_ws_messages(ws=ws, output=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    f"Unexpected error while listening to user stream {url}. Retrying after 5 seconds..."
                )
                await self._sleep(5.0)
            finally:
                await self._on_user_stream_interruption(ws)
                if ws in self._ws_assistants:
                    self._ws_assistants.remove(ws)

    async def _connected_websocket_assistant(self, ws_url: str, ws_headers) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=ws_url, ws_headers=ws_headers, message_timeout=15)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        topics = [
            {"topic": CONSTANTS.USER_TRADE},
            {"topic": CONSTANTS.USER_ORDER_ADDED},
            {"topic": CONSTANTS.USER_ORDER_CANCELLED},
        ]
        payload = {"type": "SUBSCRIBE", "subscriptions": topics}
        await ws.send(WSJSONRequest(payload=payload))

    async def _process_ws_messages(self, ws: WSAssistant, output: asyncio.Queue):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if isinstance(data, list):
                for message in data:
                    if isinstance(message, dict):
                        output.put_nowait(message)
            elif isinstance(data, dict) and data.get("auth") == "fail":
                raise IOError("Private channel authentication failed.")

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    def _time(self):
        return time.time()

import asyncio
import logging
import time
from typing import TYPE_CHECKING, List, Optional

import hummingbot.connector.exchange.chainex.chainex_constants as CONSTANTS
import hummingbot.connector.exchange.chainex.chainex_web_utils as web_utils
from hummingbot.connector.exchange.chainex.chainex_auth import ChainEXAuth
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class ChainEXAPIUserStreamDataSource(UserStreamTrackerDataSource):

    HEARTBEAT_TIME_INTERVAL = 30.0

    _bausds_logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: ChainEXAuth,
                 trading_pairs: List[str],
                 connector: 'ChainexExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 time_synchronizer: Optional[TimeSynchronizer] = None):
        super().__init__()

        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._ws_assistants: List[WSAssistant] = []
        self._time_synchronizer = time_synchronizer

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bausds_logger is None:
            cls._bausds_logger = logging.getLogger(__name__)
        return cls._bausds_logger

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message

        :return: the timestamp of the last received message in seconds
        """
        t = 0.0
        if len(self._ws_assistants) > 0:
            t = min([wsa.last_recv_time for wsa in self._ws_assistants])
        return t

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection. With the established
        connection listens to all balance events and order updates provided by the exchange, and stores them in the
        output queue

        :param output: the queue to use to store the received messages
        """
        tasks_future = None
        try:
            tasks = []

            # await ws.connect(ws_url=self._auth.add_auth_to_url(CONSTANTS.PRIVATE_WSS_URL + CONSTANTS.BALANCE_PATH_URL, timestamp=timestamp),
            #                  ws_headers=self._auth.get_auth_headers(CONSTANTS.PRIVATE_WSS_URL + CONSTANTS.BALANCE_PATH_URL, timestamp=timestamp))

            timestamp = int(self._time_synchronizer.time() * 1e3)
            tasks.append(
                self._listen_for_user_stream_on_url(
                    url=self._auth.add_auth_to_url(CONSTANTS.PRIVATE_WSS_URL + CONSTANTS.BALANCE_PATH_URL, timestamp=timestamp),
                    headers=self._auth.get_auth_headers(CONSTANTS.PRIVATE_WSS_URL + CONSTANTS.BALANCE_PATH_URL, timestamp=timestamp),
                    output=output),
            )

            tasks_future = asyncio.gather(*tasks)
            await tasks_future

        except asyncio.CancelledError:
            tasks_future and tasks_future.cancel()
            raise

    async def _listen_for_user_stream_on_url(self, url: str, headers:str, output: asyncio.Queue):
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws = await self._connected_websocket_assistant(url, headers)
                self._ws_assistants.append(ws)
                await self._subscribe_channels(ws)
                # await ws.ping()  # to update last_recv_timestamp
                await self._process_websocket_messages(websocket_assistant=ws, queue=output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    f"Unexpected error while listening to user stream {url}. Retrying after 5 seconds..."
                )
                await self._sleep(5.0)
            finally:
                await self._on_user_stream_interruption(ws)
                ws and self._ws_assistants.remove(ws)

    async def _connected_websocket_assistant(self, ws_url: str, ws_headers) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=ws_url, ws_headers=ws_headers, message_timeout=15)
        # await self._authenticate(ws)
        return ws

    async def _authenticate(self, ws: WSAssistant):
        """
        Authenticates user to websocket
        """
        auth_payload: List[str] = self._auth.get_ws_auth_payload()
        payload = {"op": "auth", "args": auth_payload}
        login_request: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(login_request)
        response: WSResponse = await ws.receive()
        message = response.data

        if (
            message["success"] is not True
            or not message["request"]
            or not message["request"]["op"]
            or message["request"]["op"] != "auth"
        ):
            self.logger().error("Error authenticating the private websocket connection")
            raise IOError("Private websocket connection authentication failed")


    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Sends the subscription message.
        Subscribes to all balance events and order updates
        :param ws: the websocket assistant used to connect to the exchange
        """
        topics = []
        # @@ TODO MOVE THESE TO CHAINEX_CONSTANTS.PY
        topics.append({"topic": "NEW_USER_TRADE"})
        # topics.append({"topic": "ORDER_CREATED"})
        topics.append({"topic": "ORDER_ADDED"})
        topics.append({"topic": "ORDER_CANCELLED"})
        payload = {
            "type": "SUBSCRIBE",
            "subscriptions": topics
        }
        subscribe_message: WSJSONRequest = WSJSONRequest(payload=payload)
        await ws.send(subscribe_message)


    async def _process_ws_messages(self, ws: WSAssistant, output: asyncio.Queue):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if isinstance(data, list):
                for message in data:
                    #if message["topic"] in ["NEW_USER_TRADE", "ORDER_CREATED", "PENDING_ORDER_CANCEL", "ORDER_CANCELLED"]:
                    output.put_nowait(message)
            elif data.get("auth") == "fail":
                raise IOError("Private channel authentication failed.")

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    def _time(self):
        return time.time()

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue)
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"op": "ping"})
                await websocket_assistant.send(ping_request)
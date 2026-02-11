import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

from bidict import bidict

import hummingbot.connector.exchange.chainex.chainex_constants as CONSTANTS
from hummingbot.connector.exchange.chainex import chainex_utils, chainex_web_utils as web_utils
from hummingbot.connector.exchange.chainex.chainex_order_book import ChainEXOrderBook
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.chainex.chainex_exchange import ChainEXExchange


class ChainEXAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(self,
                 trading_pairs: List[str],
                 connector: "ChainEXExchange",
                 api_factory: WebAssistantsFactory,
                 time_synchronizer: Optional[TimeSynchronizer] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 throttler: Optional[AsyncThrottler] = None,):
        super().__init__(trading_pairs)
        self._connector = connector
        self._time_synchronizer = time_synchronizer
        self._message_queue = defaultdict(asyncio.Queue)
        self._order_book_create_function = lambda: OrderBook()
        self._throttler = throttler
        self._domain = domain
        self._api_factory = api_factory

    @classmethod
    def trading_pair_symbol_map_ready(cls, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        return domain in cls._trading_pair_symbol_map and len(cls._trading_pair_symbol_map[domain]) > 0

    @classmethod
    async def trading_pair_symbol_map(cls,
                                      domain: str = CONSTANTS.DEFAULT_DOMAIN,
                                      api_factory: Optional[WebAssistantsFactory] = None,
                                      throttler: Optional[AsyncThrottler] = None):
        if not cls.trading_pair_symbol_map_ready(domain=domain):
            async with cls._mapping_initialization_lock:
                if not cls.trading_pair_symbol_map_ready(domain=domain):
                    await cls._init_trading_pair_symbols(domain=domain, api_factory=api_factory, throttler=throttler)

        return cls._trading_pair_symbol_map[domain]

    @staticmethod
    async def trading_pair_associated_to_exchange_symbol(symbol: str,
                                                         domain=CONSTANTS.DEFAULT_DOMAIN,
                                                         api_factory: Optional[WebAssistantsFactory] = None,
                                                         throttler: Optional[AsyncThrottler] = None) -> str:
        symbol_map = await ChainEXAPIOrderBookDataSource.trading_pair_symbol_map(
            domain=domain,
            api_factory=api_factory,
            throttler=throttler,
        )
        return symbol_map[symbol]

    @staticmethod
    async def exchange_symbol_associated_to_pair(trading_pair: str,
                                                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                                                 api_factory: Optional[WebAssistantsFactory] = None,
                                                 throttler: Optional[AsyncThrottler] = None) -> str:
        symbol_map = await ChainEXAPIOrderBookDataSource.trading_pair_symbol_map(
            domain=domain,
            api_factory=api_factory,
            throttler=throttler,
        )
        return symbol_map.inverse[trading_pair]

    @classmethod
    async def _init_trading_pair_symbols(cls,
                                         domain: str = CONSTANTS.DEFAULT_DOMAIN,
                                         api_factory: Optional[WebAssistantsFactory] = None,
                                         throttler: Optional[AsyncThrottler] = None):
        mapping = bidict()
        exchange_info = await web_utils.api_request(
            path=CONSTANTS.EXCHANGE_INFO_PATH_URL,
            api_factory=api_factory,
            throttler=throttler,
            domain=domain,
            method=RESTMethod.GET,
            limit_id=CONSTANTS.EXCHANGE_INFO_PATH_URL,
        )
        for symbol_data in filter(chainex_utils.is_exchange_information_valid, exchange_info.get("data", [])):
            exchange_symbol = symbol_data["market"]
            mapping[exchange_symbol] = f"{symbol_data['code']}-{symbol_data['exchange']}"

        cls._trading_pair_symbol_map[domain] = mapping

    async def get_snapshot(self, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        response = await web_utils.api_request(
            path=CONSTANTS.SNAPSHOT_PATH_URL + (trading_pair.replace('-', '/') + '/ALL'),
            api_factory=self._api_factory,
            throttler=self._throttler,
            domain=self._domain,
            method=RESTMethod.GET,
            limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )

        if "data" not in response:
            raise IOError(f"Error fetching market snapshot for {trading_pair}. Response: {response}.")
        return response

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair, 1000)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = ChainEXOrderBook.snapshot_message_from_exchange(
            msg=snapshot,
            timestamp=snapshot_timestamp,
            metadata={"trading_pair": trading_pair},
        )

        order_book = self.order_book_create_function()
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        message_queue = self._message_queue[CONSTANTS.ORDERBOOK_UPDATE]
        while True:
            try:
                json_msg = await message_queue.get()
                if "success" in json_msg:
                    continue
                trading_pair = await ChainEXAPIOrderBookDataSource.trading_pair_associated_to_exchange_symbol(
                    symbol=json_msg.get("pair", ""),
                    domain=self._domain,
                    api_factory=self._api_factory,
                    throttler=self._throttler,
                )
                order_book_message = ChainEXOrderBook.diff_message_from_exchange(
                    json_msg, time.time(), {"trading_pair": trading_pair}
                )
                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot = await self.get_snapshot(trading_pair=trading_pair)
                        snapshot_timestamp = time.time()
                        snapshot_msg = ChainEXOrderBook.snapshot_message_from_exchange(
                            msg=snapshot,
                            timestamp=snapshot_timestamp,
                            metadata={"trading_pair": trading_pair},
                        )
                        output.put_nowait(snapshot_msg)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error(
                            f"Unexpected error fetching order book snapshot for {trading_pair}.",
                            exc_info=True,
                        )
                        await self._sleep(5.0)
                await self._sleep(self.ONE_HOUR)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await self._sleep(5.0)

    async def listen_for_subscriptions(self):
        ws = None
        while True:
            try:
                ws = await self._api_factory.get_ws_assistant()
                await ws.connect(
                    ws_url=web_utils.public_ws_url(domain=self._domain),
                    ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL,
                )
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    data = ws_response.data
                    if not isinstance(data, dict):
                        continue
                    if "success" in data:
                        continue
                    event_type = data.get("topic")
                    if event_type in [CONSTANTS.ORDERBOOK_TRADE, CONSTANTS.ORDERBOOK_UPDATE]:
                        self._message_queue[event_type].put_nowait(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                if ws is not None:
                    await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            pairs = [trading_pair.replace('-', '/') for trading_pair in self._trading_pairs]
            payload = {"type": "SUBSCRIBE", "pairs": pairs}
            await ws.send(WSJSONRequest(payload=payload))
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True,
            )
            raise

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        message_queue = self._message_queue[CONSTANTS.ORDERBOOK_TRADE]
        while True:
            try:
                json_msg = await message_queue.get()
                timestamp = time.time()
                trades_data = json_msg if isinstance(json_msg, list) else json_msg.get("data", [])
                for trade in trades_data:
                    trading_pair = await ChainEXAPIOrderBookDataSource.trading_pair_associated_to_exchange_symbol(
                        symbol=trade["pair"],
                        domain=self._domain,
                        api_factory=self._api_factory,
                        throttler=self._throttler,
                    )
                    trade_msg = ChainEXOrderBook.trade_message_from_exchange(trade, {"trading_pair": trading_pair}, timestamp)
                    output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange")

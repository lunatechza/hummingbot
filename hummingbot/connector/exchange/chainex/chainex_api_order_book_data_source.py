import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional

from bidict import bidict

import hummingbot.connector.exchange.chainex.chainex_constants as CONSTANTS
from hummingbot.connector.exchange.chainex import (
    chainex_utils,
    chainex_web_utils as web_utils
)
from hummingbot.connector.exchange.chainex.chainex_order_book import ChainEXOrderBook
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.chainex.chainex_exchange import ChainexExchange


class ChainEXAPIOrderBookDataSource(OrderBookTrackerDataSource):

    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Mapping[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()


    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'ChainexExchange',
                 api_factory: WebAssistantsFactory,
                 time_synchronizer: Optional[TimeSynchronizer] = None,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 throttler: Optional[AsyncThrottler] = None,):
        super().__init__(trading_pairs)
        self._connector = connector
        self._time_synchronizer = time_synchronizer
        # self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        # self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._order_book_create_function = lambda: OrderBook()
        self._throttler = throttler
        self._domain = domain
        self._api_factory = api_factory


    @classmethod
    def trading_pair_symbol_map_ready(cls, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """
        Checks if the mapping from exchange symbols to client trading pairs has been initialized
        :param domain: Domain to use for the connection with the exchange
        :return: True if the mapping has been initialized, False otherwise
        """
        return domain in cls._trading_pair_symbol_map and len(cls._trading_pair_symbol_map[domain]) > 0

    @classmethod
    async def trading_pair_symbol_map(
            cls,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
            api_factory: Optional[WebAssistantsFactory] = None,
            throttler: Optional[AsyncThrottler] = None
    ):
        """
        Returns the internal map used to translate trading pairs from and to the exchange notation.
        In general this should not be used. Instead call the methods `exchange_symbol_associated_to_pair` and
        `trading_pair_associated_to_exchange_symbol`
        :param domain: Domain to use for the connection with the exchange
        :param api_factory: the web assistant factory to use in case the symbols information has to be requested
        :param throttler: the throttler instance to use in case the symbols information has to be requested
        :return: bidirectional mapping between trading pair exchange notation and client notation
        """
        if not cls.trading_pair_symbol_map_ready(domain=domain):
            async with cls._mapping_initialization_lock:
                # Check condition again (could have been initialized while waiting for the lock to be released)
                if not cls.trading_pair_symbol_map_ready(domain=domain):
                    await cls._init_trading_pair_symbols(
                        domain=domain,
                        api_factory=api_factory,
                        throttler=throttler)

        return cls._trading_pair_symbol_map[domain]

    @staticmethod
    async def trading_pair_associated_to_exchange_symbol(
            symbol: str,
            domain=CONSTANTS.DEFAULT_DOMAIN,
            api_factory: Optional[WebAssistantsFactory] = None,
            throttler: Optional[AsyncThrottler] = None) -> str:
        """
        Used to translate a trading pair from the exchange notation to the client notation
        :param symbol: trading pair in exchange notation
        :param domain: Domain to use for the connection with the exchange
        :param api_factory: the web assistant factory to use in case the symbols information has to be requested
        :param throttler: the throttler instance to use in case the symbols information has to be requested
        :return: trading pair in client notation
        """
        symbol_map = await ChainEXAPIOrderBookDataSource.trading_pair_symbol_map(
            domain=domain,
            api_factory=api_factory,
            throttler=throttler)
        return symbol_map[symbol]

    @staticmethod
    async def fetch_trading_pairs(
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
            throttler: Optional[AsyncThrottler] = None,
            api_factory: Optional[WebAssistantsFactory] = None) -> List[str]:
        """
        Returns a list of all known trading pairs enabled to operate with
        :param domain: Domain to use for the connection with the exchange
        :return: list of trading pairs in client notation
        """
        mapping = await ChainEXAPIOrderBookDataSource.trading_pair_symbol_map(
            domain=domain,
            throttler=throttler,
            api_factory=api_factory,
        )
        return list(mapping.values())

    @classmethod
    async def get_last_traded_prices(cls,
                                     trading_pairs: List[str],
                                     domain: str = CONSTANTS.DEFAULT_DOMAIN,
                                     api_factory: Optional[WebAssistantsFactory] = None,
                                     throttler: Optional[AsyncThrottler] = None) -> Dict[str, float]:
        """
        Return a dictionary the trading_pair as key and the current price as value for each trading pair passed as
        parameter
        :param trading_pairs: list of trading pairs to get the prices for
        :param domain: which CoinFLEX domain we are connecting to (the default value is 'com')
        :param api_factory: the instance of the web assistant factory to be used when doing requests to the server.
        If no instance is provided then a new one will be created.
        :param throttler: the instance of the throttler to use to limit request to the server. If it is not specified
        the function will create a new one.
        :return: Dictionary of associations between token pair and its latest price
        """
        resp = await web_utils.api_request(
            path=CONSTANTS.MARKET_SUMMARY_PATH_URL,
            api_factory=api_factory,
            throttler=throttler,
            domain=domain,
            method=RESTMethod.GET,
        )

        results = {}

        for t_pair in trading_pairs:
            symbol = await cls.trading_pair_associated_to_exchange_symbol(
                symbol=t_pair,
                domain=domain,
                throttler=throttler)
            matched_ticker = [t for t in resp if t.get("market") == symbol]
            if not (len(matched_ticker) and "last_price" in matched_ticker[0]):
                raise IOError(f"Error fetching last traded prices for {t_pair}. "
                              f"Response: {resp}.")
            results[t_pair] = float(matched_ticker[0]["last_price"])

        return results

    @classmethod
    async def _init_trading_pair_symbols(
            cls,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
            api_factory: Optional[WebAssistantsFactory] = None,
            throttler: Optional[AsyncThrottler] = None):
        """
        Initialize mapping of trade symbols in exchange notation to trade symbols in client notation
        """
        mapping = bidict()

        try:
            data = await web_utils.api_request(
                path=CONSTANTS.MARKET_SUMMARY_PATH_URL,
                api_factory=api_factory,
                throttler=throttler,
                domain=domain,
                method=RESTMethod.GET,
            )

            for symbol_data in data["data"]:
                mapping[f"{symbol_data['code']}/{symbol_data['exchange']}"] = f"{symbol_data['code']}-{symbol_data['exchange']}"
        except Exception as ex:
            cls.logger().error(f"There was an error requesting exchange info ({str(ex)})")

        cls._trading_pair_symbol_map[domain] = mapping

    @staticmethod
    async def exchange_symbol_associated_to_pair(
            trading_pair: str,
            domain=CONSTANTS.DEFAULT_DOMAIN,
            api_factory: Optional[WebAssistantsFactory] = None,
            throttler: Optional[AsyncThrottler] = None,
    ) -> str:
        """
        Used to translate a trading pair from the client notation to the exchange notation
        :param trading_pair: trading pair in client notation
        :param domain: Domain to use for the connection with the exchange
        :param api_factory: the web assistant factory to use in case the symbols information has to be requested
        :param throttler: the throttler instance to use in case the symbols information has to be requested
        :return: trading pair in exchange notation
        """
        symbol_map = await ChainEXAPIOrderBookDataSource.trading_pair_symbol_map(
            domain=domain,
            api_factory=api_factory,
            throttler=throttler)
        return symbol_map.inverse[trading_pair]

    async def get_snapshot(
            self,
            trading_pair: str,
            limit: int = 1000,
    ) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved
        :param limit: the depth of the order book to retrieve

        :return: the response from the exchange (JSON dictionary)
        """
        response = await web_utils.api_request(
            path=format(CONSTANTS.SNAPSHOT_PATH_URL + (trading_pair.replace('-','/')+ '/ALL')),
            api_factory=self._api_factory,
            throttler=self._throttler,
            domain=self._domain,
            method=RESTMethod.GET,
            limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )

        if not ("data" in response):
            raise IOError(f"Error fetching market snapshot for {trading_pair}. "
                          f"Response: {response}.")
        return response

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Creates a local instance of the exchange order book for a particular trading pair
        :param trading_pair: the trading pair for which the order book has to be retrieved
        :return: a local copy of the current order book in the exchange
        """
        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair, 1000)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = ChainEXOrderBook.snapshot_message_from_exchange(
            msg=snapshot,
            timestamp=snapshot_timestamp,
            metadata={"trading_pair": trading_pair},
        )

        order_book = self.order_book_create_function()
        print((order_book))
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # @@ TODO Complete this
        """
        Reads the order diffs events queue. For each event creates a diff message instance and adds it to the
        output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        """ message_queue = self._message_queue[CONSTANTS.ORDERBOOK_UPDATE]
        while True:
            try:
                json_msg = await message_queue.get()
                if "success" in json_msg:
                    continue
                trading_pair = await ChainEXAPIOrderBookDataSource.trading_pair_associated_to_exchange_symbol(
                    symbol=json_msg["data"]["pair"],
                    domain=self._domain,
                    api_factory=self._api_factory,
                    throttler=self._throttler)
                order_book_message: OrderBookMessage = ChainEXOrderBook.diff_message_from_exchange(
                    json_msg, time.time(), {"trading_pair": trading_pair})
                output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange") """

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        # @@ TODO Complete this
        """
        This method runs continuously and request the full order book content from the exchange every hour.
        The method uses the REST API from the exchange because it does not provide an endpoint to get the full order
        book through websocket. With the information creates a snapshot messages that is added to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        """ while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, Any] = await self.get_snapshot(trading_pair=trading_pair)
                        snapshot_timestamp: float = time.time()
                        snapshot_msg: OrderBookMessage = ChainEXOrderBook.snapshot_message_from_exchange(
                            msg=snapshot,
                            timestamp=snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().error(f"Unexpected error fetching order book snapshot for {trading_pair}.",
                                            exc_info=True)
                        await self._sleep(5.0)
                await self._sleep(self.ONE_HOUR)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await self._sleep(5.0)
 """
    async def listen_for_subscriptions(self):
        # @@ TODO Test and complete this
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        """
        ws = None
        while True:
            try:
                ws: WSAssistant = await self._api_factory.get_ws_assistant()
                await ws.connect(ws_url=CONSTANTS.PUBLIC_WSS_URL,
                                 ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    print(ws_response)
                    ProcessLookupError(ws_response.data)
                    data = ws_response.data
                    if "success" in data:
                        continue
                    event_type = data.get("topic")
                    if event_type in [CONSTANTS.ORDERBOOK_TRADE, CONSTANTS.ORDERBOOK_UPDATE]:
                        self._message_queue[event_type].put_nowait(data)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                print(err)
                self.logger().error(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect() """

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            pairs = []
            for trading_pair in self._trading_pairs:
                # symbol = await self.exchange_symbol_associated_to_pair(
                #     trading_pair=trading_pair,
                #     domain=self._domain,
                #     api_factory=self._api_factory,
                #     throttler=self._throttler)
                pairs.append(trading_pair.replace('-','/'))

            payload: Dict[str, str] = {
                "type": "SUBSCRIBE",
                "pairs": pairs,
            }
            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)

            await ws.send(subscribe_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the trade events queue. For each event creates a trade message instance and adds it to the output queue
        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created trade messages
        """
        message_queue = self._message_queue[CONSTANTS.ORDERBOOK_TRADE]
        while True:
            try:
                trades_data = await message_queue.get()

                timestamp: float = time.time()
                for json_msg in trades_data:
                    trading_pair = await ChainEXAPIOrderBookDataSource.trading_pair_associated_to_exchange_symbol(
                        symbol=json_msg["pair"],
                        domain=self._domain,
                        api_factory=self._api_factory,
                        throttler=self._throttler)
                    trade_msg: OrderBookMessage = ChainEXOrderBook.trade_message_from_exchange(
                        json_msg, {"trading_pair": trading_pair}, timestamp)
                    output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange")

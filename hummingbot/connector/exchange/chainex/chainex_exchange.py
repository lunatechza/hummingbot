import asyncio
import time
import datetime
import aiohttp
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.chainex import (
    chainex_constants as CONSTANTS,
    chainex_utils,
    chainex_web_utils as web_utils,
)
from hummingbot.connector.exchange.chainex.chainex_api_order_book_data_source import ChainEXAPIOrderBookDataSource
from hummingbot.connector.exchange.chainex.chainex_api_user_stream_data_source import ChainEXAPIUserStreamDataSource
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.exchange.chainex.chainex_auth import ChainEXAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None


class ChainexExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 chainex_api_key: str,
                 chainex_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = chainex_api_key
        self.secret_key = chainex_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_chainex_timestamp = 1.0

        self._throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS, client_config_map.rate_limits_share_pct)
        self._time_synchronizer =  TimeSynchronizer()
        self._auth = self.authenticator
        self._web_assistants_factory = self._create_web_assistants_factory()
        self._user_stream_tracker = UserStreamTracker(data_source=self._create_user_stream_data_source())
        self._user_stream_tracker_task = None

        super().__init__(client_config_map)

    @staticmethod
    def chainex_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(chainex_type: str) -> OrderType:
        return OrderType[chainex_type]

    @property
    def authenticator(self):
        return ChainEXAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        if self._domain == "com":
            return "chainex"
        else:
            return f"chainex_{self._domain}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def start_network(self):
        """
        This function is required by the NetworkIterator base class and is called automatically.
        It starts tracking order books, polling trading rules, updating statuses, and tracking user data.
        """
        self.order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        # @@ TODO NYI
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)
        return pairs_prices

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> ChainEXAPIOrderBookDataSource:
        return ChainEXAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> ChainEXAPIUserStreamDataSource:
        return ChainEXAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
            time_synchronizer=self._time_synchronizer
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        # ChainEX only supports placing limit orders on the public API
        order_result = None
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        split_pair = trading_pair.split('-');
        coin = split_pair[0]
        exchange = split_pair[1]
        api_params = {
                      "coin": coin,
                      "exchange": exchange,
                      "price": price_str,
                      "amount": amount_str,
                      "type": trade_type.value, # @@ TODO CHECK IF BUY/SELL SIDE IS WORKING AS EXPECTED
                      }

        order_result = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True)

        if not order_result["status"] == "success":
            raise IOError(f"Error submitting order {order_id}.")

        o_id = str(order_result["data"]["order_id"])
        transact_time = float(order_result["data"]["time"]) * 1e-3
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        cancel_result = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL + tracked_order['exchange_order_id'],
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_PATH_URL
            )
        if cancel_result.get("status") == "success":
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        # Not available, create default rules
        return_val: list = []
        rules: list = exchange_info_dict.get("data", [])
        for rule in rules:
            trading_pair = rule["code"] + ('-' + rule["exchange"])
            return_val.append(
                    TradingRule(
                        trading_pair,
                        min_order_value = 0.0001,
                        min_base_amount_increment= 0.00000001,
                        min_price_increment= 0.00000001,
                        supports_market_orders= False
                    )
                )
        return return_val

    # async def _status_polling_loop_fetch_updates(self):
    #     await self._update_order_fills_from_trades()
    #     await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        # Not available
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        # @@ TODO TEST AND COMPLETE THESE ACCOUNT UPDATES
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("topic")
                client_order_id = event_message.get("orderId")
                timestamp = str(self._time_synchronizer.time() * 1e3)

                if event_type == "NEW_USER_TRADE":
                    tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                    if tracked_order is not None:
                        trade_update = TradeUpdate(
                            trade_id=str(event_message.get("tradeId")),
                            client_order_id=client_order_id,
                            trading_pair=tracked_order.trading_pair,
                            fee=event_message["fee"],
                            fill_base_amount=Decimal(event_message["amount"]),
                            fill_quote_amount=Decimal(event_message["amount"]),
                            fill_price=Decimal(event_message["price"]),
                            fill_timestamp=timestamp,
                        )
                        self._order_tracker.process_trade_update(trade_update)
                elif event_type == "ORDER_ADDED":
                    order_update = OrderUpdate(
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=timestamp,
                        client_order_id=client_order_id,
                        new_state=OrderState.CREATED
                    )
                    self._order_tracker.process_order_update(order_update)
                elif event_type == "ORDER_CANCELLED":
                    tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=timestamp,
                            client_order_id=client_order_id,
                            new_state=OrderState.CANCELED
                        )
                        self._order_tracker.process_order_update(order_update)
                # elif event_type == "BALANCE_UPDATE":
                #     balances = event_message["data"]
                #     for balance_entry in balances:
                #         asset_name = balance_entry["coin"]
                #         free_balance = Decimal(balance_entry["balanceAvailable"])

                #         total_balance = Decimal(balance_entry["balance_available"]) + Decimal(balance_entry["balance_pending_deposit"])
                #         total_balance += Decimal(balance_entry["balance_pending_withdraw"]) + Decimal(balance_entry["balance_held"])
                #         self._account_available_balances[asset_name] = free_balance
                #         self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)


    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # ChainEX does not have an endpoint to retrieve trades for a particular order
        pass

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        print(CONSTANTS.TRADING_PATH_URL)
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.TRADING_PATH_URL + tracked_order['exchange_order_id'],
            params={},
            limit_id=CONSTANTS.TRADING_PATH_URL,
            is_auth_required=True
            )

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["order_id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data["time"] * 1e-3,
        )

        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.BALANCE_PATH_URL,
            is_auth_required=True)

        balances = account_info["data"]
        for balance_entry in balances:
            asset_name = balance_entry["code"]
            free_balance = Decimal(balance_entry["balance_available"])
            total_balance = Decimal(balance_entry["balance_available"]) + Decimal(balance_entry["balance_pending_deposit"])
            total_balance += Decimal(balance_entry["balance_pending_withdraw"]) + Decimal(balance_entry["balance_held"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()

        for symbol_data in filter(chainex_utils.is_exchange_information_valid, exchange_info["data"]):
            exchange_symbol = symbol_data["market"]
            mapping[exchange_symbol] = combine_to_hb_trading_pair(base=symbol_data["code"],
                                                                        quote=symbol_data["exchange"])

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        # params = {
        #     "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        # }

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL + trading_pair.replace('-', '/')
            # params=params
        )

        return float(resp_json["last_price"])

    async def _api_request(self,
                           path_url,
                           method: RESTMethod = RESTMethod.GET,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None,
                           is_auth_required: bool = False,
                           return_err: bool = False,
                           limit_id: Optional[str] = None,
                           trading_pair: Optional[str] = None,
                           **kwargs) -> Dict[str, Any]:
        last_exception = None
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = web_utils.public_rest_url(path_url, domain=self.domain)
        local_headers = {
            "Content-Type": "application/json"}
        for _ in range(2):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
                    # data=aiohttp.FormData(data) if method == RESTMethod.POST and params is not None else None,
                    # data=bytes(str(data).encode()),
                    data=data,
                    method=method,
                    is_auth_required=is_auth_required,
                    return_err=return_err,
                    headers=local_headers,
                    throttler_limit_id=limit_id if limit_id else path_url,
                )
                return request_result
            except IOError as request_exception:
                last_exception = request_exception
                if self._is_request_exception_related_to_time_synchronizer(request_exception=request_exception):
                    self._time_synchronizer.clear_time_offset_ms_samples()
                    await self._update_time_synchronizer()
                else:
                    raise

        # Failed even after the last retry
        raise last_exception

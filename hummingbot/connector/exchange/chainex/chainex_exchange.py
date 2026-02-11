import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.chainex import chainex_constants as CONSTANTS, chainex_utils, chainex_web_utils as web_utils
from hummingbot.connector.exchange.chainex.chainex_api_order_book_data_source import ChainEXAPIOrderBookDataSource
from hummingbot.connector.exchange.chainex.chainex_api_user_stream_data_source import ChainEXAPIUserStreamDataSource
from hummingbot.connector.exchange.chainex.chainex_auth import ChainEXAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class ChainEXExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
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
        self._time_synchronizer = TimeSynchronizer()
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
        return ChainEXAuth(api_key=self.api_key, secret_key=self.secret_key, time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        if self._domain == CONSTANTS.DEFAULT_DOMAIN:
            return "chainex"
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
        self.order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        return await self._api_get(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        return "Timestamp for this request" in error_description

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> ChainEXAPIOrderBookDataSource:
        return ChainEXAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            throttler=self._throttler,
        )

    def _create_user_stream_data_source(self) -> ChainEXAPIUserStreamDataSource:
        return ChainEXAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
            time_synchronizer=self._time_synchronizer,
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
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        coin, exchange = trading_pair.split("-")
        api_params = {
            "coin": coin,
            "exchange": exchange,
            "price": price_str,
            "amount": amount_str,
            "type": trade_type.name.lower(),
        }

        order_result = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True,
        )

        if order_result.get("status") != "success":
            raise IOError(f"Error submitting order {order_id}.")

        data = order_result.get("data", {})
        o_id = str(data.get("order_id", ""))
        transact_time = float(data.get("time", self.current_timestamp * 1e3)) * 1e-3
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        if tracked_order.exchange_order_id is None:
            raise IOError(f"Failed to cancel order {order_id}, exchange order id not available.")

        cancel_result = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL + tracked_order.exchange_order_id,
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_PATH_URL,
        )
        return cancel_result.get("status") == "success"

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        return_val: list = []
        rules: list = exchange_info_dict.get("data", [])
        for rule in rules:
            trading_pair = f"{rule['code']}-{rule['exchange']}"
            return_val.append(
                TradingRule(
                    trading_pair,
                    min_order_value=Decimal("0.0001"),
                    min_base_amount_increment=Decimal("0.00000001"),
                    min_price_increment=Decimal("0.00000001"),
                    supports_market_orders=False,
                )
            )
        return return_val

    async def _update_trading_fees(self):
        # ChainEX does not expose dynamic fee tiers through public/private endpoints.
        # Keep connector-compatible no-op implementation.
        return

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("topic")
                client_order_id = event_message.get("orderId")
                timestamp = self._time_synchronizer.time()
                tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)

                if event_type == CONSTANTS.USER_TRADE and tracked_order is not None:
                    fill_amount = Decimal(str(event_message.get("amount", "0")))
                    fill_price = Decimal(str(event_message.get("price", "0")))
                    fee = event_message.get("fee")
                    if fee is None:
                        fee = DeductedFromReturnsTradeFee(flat_fees=[])
                    trade_update = TradeUpdate(
                        trade_id=str(event_message.get("tradeId")),
                        client_order_id=client_order_id,
                        exchange_order_id=tracked_order.exchange_order_id,
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_amount,
                        fill_quote_amount=fill_amount * fill_price,
                        fill_price=fill_price,
                        fill_timestamp=timestamp,
                    )
                    self._order_tracker.process_trade_update(trade_update)

                elif event_type == CONSTANTS.USER_ORDER_ADDED and tracked_order is not None:
                    self._order_tracker.process_order_update(
                        OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=timestamp,
                            client_order_id=client_order_id,
                            exchange_order_id=tracked_order.exchange_order_id,
                            new_state=OrderState.OPEN,
                        )
                    )

                elif event_type == CONSTANTS.USER_ORDER_CANCELLED and tracked_order is not None:
                    self._order_tracker.process_order_update(
                        OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=timestamp,
                            client_order_id=client_order_id,
                            exchange_order_id=tracked_order.exchange_order_id,
                            new_state=OrderState.CANCELED,
                        )
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []

        coin = order.trading_pair.split("-")[0]
        response = await self._api_get(
            path_url=f"{CONSTANTS.MY_TRADES_PATH_URL}{coin}",
            is_auth_required=True,
            limit_id=CONSTANTS.MY_TRADES_PATH_URL,
        )

        trade_updates = []
        for trade in response.get("data", []):
            if str(trade.get("order_id")) != str(order.exchange_order_id):
                continue
            fill_base_amount = Decimal(str(trade.get("amount", "0")))
            fill_price = Decimal(str(trade.get("price", "0")))
            fee_amount = Decimal(str(trade.get("fee", "0")))
            fee_asset = trade.get("fee_currency", order.quote_asset)
            trade_updates.append(
                TradeUpdate(
                    trade_id=str(trade.get("trade_id")),
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    trading_pair=order.trading_pair,
                    fee=TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=fee_asset,
                        flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)],
                    ),
                    fill_base_amount=fill_base_amount,
                    fill_quote_amount=fill_base_amount * fill_price,
                    fill_price=fill_price,
                    fill_timestamp=float(trade.get("time", self.current_timestamp * 1e3)) * 1e-3,
                )
            )
        return trade_updates

    def _order_state_from_exchange(self, status: str) -> OrderState:
        mapped = CONSTANTS.ORDER_STATE.get(str(status).lower())
        if mapped is None:
            return OrderState.OPEN
        return getattr(OrderState, mapped)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        if tracked_order.exchange_order_id is None:
            raise IOError(f"In-flight order {tracked_order.client_order_id} has no exchange order id.")

        response = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL + tracked_order.exchange_order_id,
            params={},
            limit_id=CONSTANTS.ORDER_PATH_URL,
            is_auth_required=True,
        )
        updated_order_data = response.get("data", response)

        exchange_order_id = str(updated_order_data.get("order_id", tracked_order.exchange_order_id))
        update_timestamp = float(updated_order_data.get("time", self.current_timestamp * 1e3)) * 1e-3
        order_status = updated_order_data.get("status", "open")
        new_state = self._order_state_from_exchange(order_status)

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=update_timestamp,
            new_state=new_state,
        )

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(path_url=CONSTANTS.BALANCE_PATH_URL, is_auth_required=True)

        balances = account_info.get("data", [])
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
        for symbol_data in filter(chainex_utils.is_exchange_information_valid, exchange_info.get("data", [])):
            exchange_symbol = symbol_data["market"]
            mapping[exchange_symbol] = combine_to_hb_trading_pair(base=symbol_data["code"], quote=symbol_data["exchange"])

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL + trading_pair.replace("-", "/"),
        )

        data = resp_json.get("data", resp_json)
        return float(data.get("last_price"))

    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        for order in orders:
            if order.exchange_order_id is None:
                continue

            trade_updates = await self._all_trade_updates_for_order(order=order)
            for trade_update in trade_updates:
                self._order_tracker.process_trade_update(trade_update=trade_update)

    async def _update_orders_with_error_handler(self, orders: List[InFlightOrder], error_handler):
        for order in orders:
            try:
                order_update = await self._request_order_status(order)
                self._order_tracker.process_order_update(order_update)
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                await error_handler(order, ex)

    async def _user_stream_event_listener_task(self):
        await self._user_stream_event_listener()

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
        local_headers = {"Content-Type": "application/json"}
        for _ in range(2):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
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

        raise last_exception


# Backward-compatible alias for existing imports
ChainexExchange = ChainEXExchange

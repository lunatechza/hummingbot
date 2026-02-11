from decimal import Decimal
from typing import Any, Dict, List, Tuple

from pydantic import Field, SecretStr

import hummingbot.connector.exchange.chainex.chainex_constants as CONSTANTS
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookRow

CENTRALIZED = True
EXAMPLE_PAIR = "BTC/USDT"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("-0.01"),
    taker_percent_fee_decimal=Decimal("0.1"),
)

def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    """
    Creates a client order id for a new order
    :param is_buy: True if the order is a buy order, False otherwise
    :param trading_pair: the trading pair the order will be operating with
    :return: an identifier for the new order to be used in the client
    """
    side = "0" if is_buy else "1"
    return f"{CONSTANTS.HBOT_ORDER_ID_PREFIX}{side}{get_tracking_nonce()}"


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("market", False)

def decimal_val_or_none(string_value: str):
    return Decimal(string_value) if string_value else None

class ChainEXConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="chainex", const=True, client_data=None)
    chainex_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your ChainEX API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    chainex_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your ChainEX API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )

    class Config:
        title = "chainex"


KEYS = ChainEXConfigMap.construct()

class ChainEXTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="chainex_testnet", const=True, client_data=None)
    chainex_testnet_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your ChainEX Testnet API Key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )
    chainex_testnet_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your ChainEX Testnet API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "chainex_testnet"

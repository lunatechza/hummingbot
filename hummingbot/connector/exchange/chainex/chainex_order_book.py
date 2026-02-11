from decimal import Decimal
from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class ChainEXOrderBook(OrderBook):

    @staticmethod
    def _side_orders(data: list, side: str):
        side_entries = [entry for entry in data if entry.get("type") == side]
        if not side_entries:
            return []
        return side_entries[0].get("orders", [])

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        data = msg.get("data", [])
        buy_orders = cls._side_orders(data, "buy")
        sell_orders = cls._side_orders(data, "sell")

        bids = [[Decimal(buy["price"]), Decimal(buy["amount"]), timestamp] for buy in buy_orders]
        asks = [[Decimal(sell["price"]), Decimal(sell["amount"]), timestamp] for sell in sell_orders]

        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": msg["trading_pair"],
                "bids": bids,
                "asks": asks,
                "update_id": int(timestamp * 1e3),
            },
            timestamp=timestamp,
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)

        data = msg.get("data", {})
        ts = timestamp if timestamp is not None else 0

        bids = [[Decimal(level[0]), Decimal(level[1]), ts] for level in data.get("bids", [])]
        asks = [[Decimal(level[0]), Decimal(level[1]), ts] for level in data.get("asks", [])]

        return OrderBookMessage(
            OrderBookMessageType.DIFF,
            {
                "trading_pair": msg.get("trading_pair", msg.get("pair", "")),
                "bids": bids,
                "asks": asks,
                "update_id": int(ts * 1e3),
            },
            timestamp=ts,
        )

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, any],
                                    metadata: Optional[Dict] = None,
                                    timestamp: Optional[float] = None):
        if metadata:
            msg.update(metadata)

        trade_ts = msg.get("timestamp")
        if trade_ts is None:
            trade_ts = int((timestamp or 0) * 1e3)

        side = msg.get("type")
        trade_type = float(TradeType.BUY.value)
        if str(side).lower() in {"sell", "1", "s"}:
            trade_type = float(TradeType.SELL.value)

        amount = msg.get("amount", msg.get("quantity", "0"))

        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": msg.get("trading_pair", msg.get("pair", "")),
                "trade_type": trade_type,
                "price": msg["price"],
                "amount": amount,
                "update_id": int(trade_ts),
            },
            timestamp=int(trade_ts) * 1e-3,
        )

from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.connector.exchange.chainex import (chainex_utils)
from hummingbot.core.data_type.order_book_row import OrderBookRow

class ChainEXOrderBook(OrderBook):

    # Retrieve all asks (referred to as buy orders)
    def buyFilter(element):
            return element["type"] == "buy"

    # Retrieve all bids (referred to as sell orders)
    def sellFilter(element):
        return element["type"] == "sell"

    @classmethod
    def snapshot_message_from_exchange(self,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a snapshot message with the order book snapshot message
        :param msg: the response from the exchange when requesting the order book snapshot
        :param timestamp: the snapshot timestamp
        :param metadata: a dictionary with extra information to add to the snapshot data
        :return: a snapshot message with the snapshot information received from the exchange
        """
        if metadata:
            msg.update(metadata)

        data = msg["data"]

        buys = list(filter(self.buyFilter, data))
        buys = buys[0]['orders']
        sells = list(filter(self.sellFilter, data))
        sells = sells[0]['orders']
        sellMap = []
        buyMap = []
        for buy in buys:
            buyMap.append([buy['price'], buy['amount'], timestamp]) # [price, amount, update_id]
        for sell in sells:
            sellMap.append([sell['price'], sell['amount'], timestamp])

        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg['trading_pair'],
            "bids": sellMap,
            "asks": buyMap,
            "update_id": timestamp
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(self,
                                   cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Creates a diff message with the changes in the order book received from the exchange
        :param msg: the changes in the order book
        :param timestamp: the timestamp of the difference
        :param metadata: a dictionary with extra information to add to the difference data
        :return: a diff message with the changes in the order book notified by the exchange
        """

        if metadata:
            msg.update(metadata)

        data = msg["data"]
        # @@ TODO: CHANGE THIS TO WORK LIKE snapshot_message_from_exchange, WHERE BIDS = SELLS, ASKS = BUYS

        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["pair"],
            "bids": data['bids'],
            "asks": data['asks'],
            "update_id": timestamp
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None, timestamp: Optional[float] = None,):
        # @@ TODO TEST AND COMPLETE THIS
        """
        Creates a trade message with the information from the trade event sent by the exchange
        :param msg: the trade event details sent by the exchange
        :param metadata: a dictionary with extra information to add to trade message
        :return: a trade message with the details of the trade as provided by the exchange
        """
        if metadata:
            msg.update(metadata)

        ts = int(msg["timestamp"])

        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["pair"],
            # "trade_type": float(TradeType.SELL.value) if msg["type"] == 1 else float(TradeType.BUY.value),
            "trade_type": float(TradeType.SELL.value) if msg["type"] == 1 else float(TradeType.BUY.value),
            "price": msg["price"],
            "amount": msg["quantity"],
            "update_id": timestamp
        }, timestamp=ts * 1e-3)

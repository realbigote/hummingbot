#!/usr/bin/env python
import logging
from typing import (
    Dict,
    Optional
)
import ujson

from aiokafka import ConsumerRecord
from sqlalchemy.engine import RowProxy

from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType
)

_bob_logger = None


cdef class BlocktaneOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _bob_logger
        if _bob_logger is None:
            _bob_logger = logging.getLogger(__name__)
        return _bob_logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": timestamp,  # not sure whether this is correct
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["pair"],
            "update_id": timestamp,
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=timestamp)

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = record["json"] if type(record["json"])==dict else ujson.loads(record["json"])
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["pair"],
            "update_id": record.timestamp,
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=record["timestamp"] * 1e-3)

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record["json"])
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["pair"],
            "update_id": record.timestamp,
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=record["timestamp"] * 1e-3)

    @classmethod
    def snapshot_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode("utf-8"))
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["pair"],
            "update_id": record.timestamp,
            "bids": msg["bids"],
            "asks": msg["asks"]
        }, timestamp=record.timestamp * 1e-3)

    @classmethod
    def diff_message_from_kafka(cls, record: ConsumerRecord, metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = ujson.loads(record.value.decode("utf-8"))
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["pair"],
            "update_id": record.timestamp,
            "bids": msg["bids"],
            "asks": msg["asks"],

        }, timestamp=record.timestamp * 1e-3)

    @classmethod
    def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        msg = record["json"]
        if metadata:
            msg.update(metadata)
        ts = record.timestamp
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["market"],
            "trade_type": msg["taker_type"],
            "trade_id": msg["id"],
            "update_id": ts,
            "price": msg["price"],
            "amount": msg["amount"]
        }, timestamp=ts * 1e-3)  # not sure about this

    @classmethod
    def trade_message_from_exchange(cls, msg: Dict[str, any], metadata: Optional[Dict] = None):
        if metadata:
            msg.update(metadata)
        ts = msg["created_at"]
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": msg["market"],
            "trade_type": msg["taker_type"],
            "trade_id": msg["id"],
            "update_id": ts,
            "price": msg["price"],
            "amount": msg["amount"]
        }, timestamp=ts * 1e-3)

    @classmethod
    def from_snapshot(cls, msg: OrderBookMessage) -> "OrderBook":
        retval = BlocktaneOrderBook()
        retval.apply_snapshot(msg.bids, msg.asks, msg.update_id)
        return retval

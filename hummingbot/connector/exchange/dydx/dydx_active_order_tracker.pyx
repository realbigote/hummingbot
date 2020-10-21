# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp

import logging

import numpy as np
import math
from decimal import Decimal

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow
from hummingbot.connector.exchange.dydx.dydx_api_token_configuration_data_source import DydxAPITokenConfigurationDataSource
from hummingbot.connector.exchange.dydx.dydx_utils import hash_order_id

s_empty_diff = np.ndarray(shape=(0, 4), dtype="float64")
_ddaot_logger = None

cdef class DydxActiveOrderTracker:
    def __init__(self, token_configuration, active_asks=None, active_bids=None):
        super().__init__()
        self._active_asks = active_asks or {}
        self._active_bids = active_bids or {}
        self._token_config: DydxAPITokenConfigurationDataSource = token_configuration

    @property
    def token_configuration(self) -> DydxAPITokenConfigurationDataSource:
        if not self._token_config:
            self._token_config = DydxAPITokenConfigurationDataSource.create()
        return self._token_config

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _ddaot_logger
        if _ddaot_logger is None:
            _ddaot_logger = logging.getLogger(__name__)
        return _ddaot_logger

    @property
    def active_asks(self):
        return self._active_asks

    @property
    def active_bids(self):
        return self._active_bids

    cdef tuple c_convert_snapshot_message_to_np_arrays(self, object message):
        cdef:
            object price
            str order_id

        # Refresh all order tracking.
        self._active_bids.clear()
        self._active_asks.clear()
        for bid_order in message.bids:
            price, amount = self.get_rates_and_quantities(bid_order["price"], bid_order["amount"], message.content["market"])
            level_id = self.hash_order_id(bid_order["id"])
            if price in self.active_bids:
                self.active_bids[price]["totalAmount"] += amount
                self.active_bids[price]["order_ids"].append(level_id)
            else:
                self.active_bids[price] = {
                    "totalAmount": amount,
                    "order_id": level_id,
                    "order_ids": [level_id]
                }

        for ask_order in message.asks:
            price, amount = self.get_rates_and_quantities(ask_order["price"], ask_order["amount"], message.content["market"])
            level_id = self.hash_order_id(ask_order["id"])
            if price in self.active_asks:
                self.active_asks[price]["totalAmount"] += amount
                self.active_asks[price]["order_ids"].append(level_id)
            else:
                self.active_asks[price] = {
                    "totalAmount": amount,
                    "order_id": level_id,
                    "order_ids": [level_id]
                }
        # Return the sorted snapshot tables.
        cdef:
            np.ndarray[np.float64_t, ndim=2] bids = np.array(
                [[message.timestamp,
                  Decimal(price),
                  Decimal(self._active_bids[price]["totalAmount"]),
                  self.active_bids[price]["order_id"]
                  ]
                 for price in sorted(self.active_bids.keys(), reverse=True)], dtype="float64", ndmin=2)

            np.ndarray[np.float64_t, ndim=2] asks = np.array(
                [[message.timestamp,
                  Decimal(price),
                  Decimal(self.active_asks[price]["totalAmount"]),
                  self.active_asks[price]["order_id"]]
                 for price in sorted(self.active_asks.keys(), reverse=False)], dtype="float64", ndmin=2)

        # If there are no rows, the shape would become (1, 0) and not (0, 4).
        # Reshape to fix that.
        if bids.shape[1] != 4:
            bids = bids.reshape((0, 4))
        if asks.shape[1] != 4:
            asks = asks.reshape((0, 4))
        return bids, asks

    def get_rates_and_quantities(self, price, amount, market) -> tuple:
        pair_tuple = tuple(market.split('-'))
        basetokenid = self.token_configuration.get_tokenid(pair_tuple[0])
        quotetokenid = self.token_configuration.get_tokenid(pair_tuple[1])
        new_price = float(self.token_configuration.unpad(self.token_configuration.pad(price,quotetokenid), basetokenid))
        new_amount = float(self.token_configuration.unpad(amount, basetokenid))
        return new_price, new_amount

    cdef tuple c_convert_diff_message_to_np_arrays(self, object message):
        cdef:
            dict content = message.content
            str msg_type = content["type"]
            str market = content["market"]
            str order_id = content["id"]
            str order_side = content["side"]
            double timestamp = message.timestamp
            double quantity = 0

        bids = s_empty_diff
        asks = s_empty_diff
        correct_price = None
        #if msg_type == "REMOVED" and order_side == "BUY":

        level_id = self.hash_order_id(content["id"])
        if msg_type == "NEW":
            price = content["price"]
            amount = content["amount"]
            correct_price, correct_amount = self.get_rates_and_quantities(price, amount, market)
            if order_side == "BUY":
                if correct_price in self.active_bids:
                    self.active_bids[correct_price]["totalAmount"] += correct_amount
                    self.active_bids[correct_price]["order_ids"].append(level_id)
                else:
                    self.active_bids[correct_price] = {
                        "totalAmount": correct_amount,
                        "order_id": level_id,
                        "order_ids": [level_id]
                    }
            else:
                if correct_price in self.active_asks:
                    self.active_asks[correct_price]["totalAmount"] += correct_amount
                    self.active_asks[correct_price]["order_ids"].append(level_id)
                else:
                    self.active_asks[correct_price] = {
                        "totalAmount": correct_amount,
                        "order_id": level_id,
                        "order_ids": [level_id]
                    }
        elif msg_type in ["UPDATED", "REMOVED"]:
            if order_side == "BUY":
                for price in self.active_bids.keys():      
                    if level_id in self.active_bids[price]["order_ids"]:
                        if msg_type == "UPDATED":
                            amount = content["amount"]
                            correct_price, correct_amount = self.get_rates_and_quantities(price, amount, market)
                        else:
                            correct_price, correct_amount = self.get_rates_and_quantities(price, 0, market)
                            self.active_bids[price]["order_ids"].remove(level_id)
            else:
                for price in self.active_asks.keys():
                    if level_id in self.active_asks[price]["order_ids"]:
                        if msg_type == "UPDATED":
                            amount = content["amount"]
                            correct_price, correct_amount = self.get_rates_and_quantities(price, amount, market)
                        else:
                            correct_price, correct_amount = self.get_rates_and_quantities(price, 0, market)
                            self.active_asks[price]["order_ids"].remove(level_id)

        if correct_price is not None:
            if order_side == "BUY":
                bids = np.array(
                    [[timestamp,
                      float(correct_price),
                      float(correct_amount),
                      level_id]],
                    dtype="float64",
                    ndmin=2
                )

            elif order_side == "SELL":
                asks = np.array(
                    [[timestamp,
                      float(correct_price),
                      float(correct_amount),
                      level_id]],
                    dtype="float64",
                    ndmin=2
                )

        return bids, asks

    def convert_diff_message_to_order_book_row(self, message):
        np_bids, np_asks = self.c_convert_diff_message_to_np_arrays(message)
        bids_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_bids]
        asks_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_asks]
        return bids_row, asks_row

    def convert_snapshot_message_to_order_book_row(self, message):
        np_bids, np_asks = self.c_convert_snapshot_message_to_np_arrays(message)
        bids_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_bids]
        asks_row = [ClientOrderBookRow(price, qty, update_id) for ts, price, qty, update_id in np_asks]
        return bids_row, asks_row

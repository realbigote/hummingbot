# distutils: language=c++
# distutils: sources=hummingbot/core/cpp/OrderBookEntry.cpp

import logging

import numpy as np
import math
from decimal import Decimal

from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_row import ClientOrderBookRow
from hummingbot.connector.exchange.dydx.dydx_api_token_configuration_data_source import DydxAPITokenConfigurationDataSource

s_empty_diff = np.ndarray(shape=(0, 4), dtype="float64")
_ddaot_logger = None

cdef class DydxActiveOrderTracker:
    def __init__(self, token_configuration, active_asks=None, active_bids=None):
        super().__init__()
        self._active_asks = active_asks or {}
        self._active_bids = active_bids or {}
        self._token_config: DydxAPITokenConfigurationDataSource = token_configuration

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
            
            if price in self._active_bids:
                self._active_bids[price]["totalAmount"] += amount
            else:
                self._active_bids[price] = {
                    "totalAmount": amount
                }

        for ask_order in message.asks:
            price, amount = self.get_rates_and_quantities(ask_order["price"], ask_order["amount"], message.content["market"])

            if price in self._active_asks:
                self._active_asks[price]["totalAmount"] += amount
            else:
                self._active_asks[price] = {
                    "totalAmount": amount
                }
        # Return the sorted snapshot tables.
        cdef:
            np.ndarray[np.float64_t, ndim=2] bids = np.array(
                [[message.timestamp,
                  Decimal(price),
                  Decimal(self._active_bids[price]["totalAmount"]),
                  message.timestamp
                  ]
                 for price in sorted(self._active_bids.keys(), reverse=True)], dtype="float64", ndmin=2)

            np.ndarray[np.float64_t, ndim=2] asks = np.array(
                [[message.timestamp,
                  Decimal(price),
                  Decimal(self._active_asks[price]["totalAmount"]),
                  message.timestamp]
                 for price in sorted(self._active_asks.keys(), reverse=True)], dtype="float64", ndmin=2)

        # If there are no rows, the shape would become (1, 0) and not (0, 4).
        # Reshape to fix that.
        if bids.shape[1] != 4:
            bids = bids.reshape((0, 4))
        if asks.shape[1] != 4:
            asks = asks.reshape((0, 4))
        return bids, asks

    def get_rates_and_quantities(self, price, amount, market) -> tuple:
        pair_tuple = tuple(market.split('-'))
        basetokenid = self._token_config.get_tokenid(pair_tuple[0])
        quotetokenid = self._token_config.get_tokenid(pair_tuple[1])
        new_price = float(self._token_config.unpad(self._token_config.pad(price,quotetokenid), basetokenid))
        new_amount = float(self._token_config.unpad(amount, basetokenid))
        return new_price, new_amount

    cdef tuple c_convert_diff_message_to_np_arrays(self, object message):
        cdef:
            dict content = message.content
            str market = content["market"]
            str order_id = content["id"]
            str order_side = content["side"]
            str price = content["price"]
            str amount = content["amount"]
            double timestamp = message.timestamp
            double quantity = 0
        bids = s_empty_diff
        asks = s_empty_diff
        
        correct_price, correct_amount = self.get_rates_and_quantities(price, amount, message.content["market"])

        if order_side == "BUY":
            bids = np.array(
                [[timestamp,
                  float(correct_price),
                  float(correct_amount),
                  int(message.timestamp)]],
                dtype="float64",
                ndmin=2
            )

        if order_side == "SELL":
            asks = np.array(
                [[timestamp,
                  float(correct_price),
                  float(correct_amount),
                  int(message.timestamp)]],
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

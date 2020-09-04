import asyncio
import logging
import time
import requests
import simplejson
from requests import Request
from decimal import Decimal
from typing import Optional, List, Dict, Any, AsyncIterable, Tuple

import aiohttp
import ujson
import pandas as pd
from async_timeout import timeout
from libc.stdint cimport int64_t

from hummingbot.core.clock cimport Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.event.events import (
    MarketEvent,
    TradeFee,
    OrderType,
    OrderFilledEvent,
    TradeType,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent, OrderCancelledEvent, MarketTransactionFailureEvent,
    MarketOrderFailureEvent, SellOrderCreatedEvent, BuyOrderCreatedEvent)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.market.ftx.ftx_api_order_book_data_source import FtxAPIOrderBookDataSource
from hummingbot.market.ftx.ftx_auth import FtxAuth
from hummingbot.market.ftx.ftx_in_flight_order import FtxInFlightOrder
from hummingbot.market.ftx.ftx_order_book_tracker import FtxOrderBookTracker
from hummingbot.market.ftx.ftx_user_stream_tracker import FtxUserStreamTracker
from hummingbot.market.deposit_info import DepositInfo
from hummingbot.market.market_base import NaN
from hummingbot.market.trading_rule cimport TradingRule
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map
from hummingbot.core.utils.estimate_fee import estimate_fee

bm_logger = None
s_decimal_0 = Decimal(0)

cdef class FtxMarketTransactionTracker(TransactionTracker):
    cdef:
        FtxMarket _owner

    def __init__(self, owner: FtxMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class FtxMarket(MarketBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDERS_INTERVAL = 10.0
    ORDER_NOT_EXIST_CONFIRMATION_COUNT = 3

    FTX_API_ENDPOINT = "https://ftx.com/api"

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global bm_logger
        if bm_logger is None:
            bm_logger = logging.getLogger(__name__)
        return bm_logger

    def __init__(self,
                 ftx_secret_key: str,
                 ftx_api_key: str,
                 poll_interval: float = 5.0,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):
        super().__init__()
        self._account_available_balances = {}
        self._account_balances = {}
        self._account_id = ""
        self._ftx_auth = FtxAuth(ftx_api_key, ftx_secret_key)
        self._data_source_type = order_book_tracker_data_source_type
        self._ev_loop = asyncio.get_event_loop()
        self._in_flight_orders = {}
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._order_book_tracker = FtxOrderBookTracker(data_source_type=order_book_tracker_data_source_type,
                                                           trading_pairs=trading_pairs)
        self._order_not_found_records = {}
        self._poll_notifier = asyncio.Event()
        self._poll_interval = poll_interval
        self._shared_client = None
        self._status_polling_task = None
        self._trading_required = trading_required
        self._trading_rules = {}
        self._trading_rules_polling_task = None
        self._tx_tracker = FtxMarketTransactionTracker(self)
        self._user_stream_event_listener_task = None
        self._user_stream_tracker = FtxUserStreamTracker(ftx_auth=self._ftx_auth,
                                                             trading_pairs=trading_pairs)
        self._user_stream_tracker_task = None
        self._check_network_interval = 60.0

    @property
    def name(self) -> str:
        return "ftx"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def ftx_auth(self) -> FtxAuth:
        return self._ftx_auth

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_book_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        self._in_flight_orders.update({
            key: FtxInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    @staticmethod
    def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
        try:
            m = trading_pair.split("/")
            return m[0], m[1]
        # Exceptions are now logged as warnings in trading pair fetcher
        except Exception as e:
            return None

    @staticmethod
    def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
        if FtxMarket.split_trading_pair(exchange_trading_pair) is None:
            return None
        # Blocktane does not split BASEQUOTE (fthusd)
        base_asset, quote_asset = FtxMarket.split_trading_pair(exchange_trading_pair)
        return f"{base_asset}-{quote_asset}".upper()
    
    @staticmethod
    def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
        return hb_trading_pair.replace("-","/")

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await FtxAPIOrderBookDataSource.get_active_exchange_markets()

    cdef c_start(self, Clock clock, double timestamp):
        self._tx_tracker.c_start(clock, timestamp)
        MarketBase.c_start(self, clock, timestamp)

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t> (self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t> (timestamp / self._poll_interval)

        MarketBase.c_tick(self, timestamp)
        self._tx_tracker.c_tick(timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        cdef:
            object maker_fee = Decimal(0.002)
            object taker_fee = Decimal(0.002)
        if order_type is OrderType.LIMIT and fee_overrides_config_map["ftx_maker_fee"].value is not None:
            return TradeFee(percent=fee_overrides_config_map["ftx_maker_fee"].value)
        if order_type is OrderType.MARKET and fee_overrides_config_map["ftx_taker_fee"].value is not None:
            return TradeFee(percent=fee_overrides_config_map["ftx_taker_fee"].value)

        return TradeFee(percent=maker_fee if order_type is OrderType.LIMIT else taker_fee)

    async def _update_balances(self):
        cdef:
            dict account_info
            list balances
            str asset_name
            set local_asset_names = set(self._account_balances.keys())
            set remote_asset_names = set()
            set asset_names_to_remove

        #path_url = "/orders"
        #orders = await self._api_request("GET", path_url=path_url)
#
        #for order in orders["result"]:
        #    await self.execute_cancel("ETH/USDT", str(order["clientId"]))

        path_url = "/wallet/balances"
        account_balances = await self._api_request("GET", path_url=path_url)

        for balance_entry in account_balances["result"]:
            asset_name = balance_entry["coin"]
            available_balance = Decimal(balance_entry["free"])
            total_balance = Decimal(balance_entry["total"])
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _format_trading_rules(self, market_dict: Dict[str, Any]) -> List[TradingRule]:
        cdef:
            list retval = []

        for market in market_dict.values():
            try:
                trading_pair = market.get("name")
                min_trade_size = Decimal(market.get("minProvideSize"))
                price_increment = Decimal(market.get("priceIncrement"))
                size_increment = Decimal(market.get("sizeIncrement"))
                min_quote_amount_increment = price_increment * size_increment
                min_order_value = min_trade_size * price_increment

                # Trading Rules info from ftx API response
                retval.append(TradingRule(trading_pair,
                                        min_order_size=Decimal(min_trade_size),
                                        min_price_increment=Decimal(price_increment),
                                        min_base_amount_increment=size_increment,
                                        min_quote_amount_increment=min_quote_amount_increment,
                                        min_order_value=Decimal(min_order_value),
                                        ))
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {market}. Skipping.", exc_info=True)
        return retval

    async def _update_trading_rules(self):
        cdef:
            # The poll interval for withdraw rules is 60 seconds.
            int64_t last_tick = <int64_t> (self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t> (self._current_timestamp / 60.0)
        if current_tick > last_tick or len(self._trading_rules) <= 0:
            market_path_url = "/markets"
            ticker_path_url = "/markets/tickers"

            market_list = await self._api_request("GET", path_url=market_path_url)

            result_list = {market["name"]: market for market in market_list["result"] if market["type"] == "spot"}

            trading_rules_list = self._format_trading_rules(result_list)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def list_orders(self) -> List[Any]:
        """
        Only a list of all currently open orders(does not include filled orders)
        :returns json response
        i.e.
        {
          "success": true,
          "result": [
            {
              "createdAt": "2019-03-05T09:56:55.728933+00:00",
              "filledSize": 10,
              "future": "XRP-PERP",
              "id": 9596912,
              "market": "XRP-PERP",
              "price": 0.306525,
              "avgFillPrice": 0.306526,
              "remainingSize": 31421,
              "side": "sell",
              "size": 31431,
              "status": "open",
              "type": "limit",
              "reduceOnly": false,
              "ioc": false,
              "postOnly": false,
              "clientId": null
            }
          ]
        }

        """
        path_url = "/orders"

        result = await self._api_request("GET", path_url=path_url)

        return result

    async def _update_order_status(self):
        cdef:
            # This is intended to be a backup measure to close straggler orders, in case ftx's user stream events
            # are not capturing the updates as intended. Also handles filled events that are not captured by
            # _user_stream_event_listener
            # The poll interval for order status is 10 seconds.
            int64_t last_tick = <int64_t>(self._last_poll_timestamp / self.UPDATE_ORDERS_INTERVAL)
            int64_t current_tick = <int64_t>(self._current_timestamp / self.UPDATE_ORDERS_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:

            tracked_orders = list(self._in_flight_orders.values())

            open_orders = await self.list_orders()
            open_orders = dict((str(entry["id"]), entry) for entry in open_orders["result"] if entry["status"] is not "closed")

            for tracked_order in tracked_orders:

                exchange_order_id = str(await tracked_order.get_exchange_order_id())
                client_order_id = tracked_order.client_order_id
                order = None
                if exchange_order_id in open_orders:
                    order = open_orders[exchange_order_id]

                # Do nothing, if the order has already been cancelled or has failed
                if client_order_id not in self._in_flight_orders:
                    continue

                if order is None:  # Handles order that are currently tracked but no longer open in exchange
                    tracked_order.last_state = "CLOSED"
                    self.c_trigger_event(
                        self.MARKET_ORDER_FAILURE_EVENT_TAG,
                        MarketOrderFailureEvent(self._current_timestamp,
                                                client_order_id,
                                                tracked_order.order_type)
                    )
                    self.c_stop_tracking_order(client_order_id)
                    self.logger().network(
                        f"Error fetching status update for the order {client_order_id}: "
                        f"{tracked_order}"
                    )
                    continue

                order_state = order["status"]
                order_type = "LIMIT" if tracked_order.order_type is OrderType.LIMIT else "MARKET"
                trade_type = "BUY" if tracked_order.trade_type is TradeType.BUY else "SELL"
                order_type_description = tracked_order.order_type_description

                executed_amount_diff = s_decimal_0

                if order["avgFillPrice"]:
                    executed_price = Decimal(order["avgFillPrice"])

                    remaining_size = Decimal(order["remainingSize"])
                    new_confirmed_amount = tracked_order.amount - remaining_size
                    executed_amount_diff = new_confirmed_amount - tracked_order.executed_amount_base
                    tracked_order.executed_amount_base = new_confirmed_amount
                    tracked_order.executed_amount_quote += executed_amount_diff * executed_price
                
                if executed_amount_diff > s_decimal_0:
                    self.logger().info(f"Filled {executed_amount_diff} out of {tracked_order.amount} of the "
                                       f"{order_type_description} order {tracked_order.client_order_id}.")
                    self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                         OrderFilledEvent(
                                             self._current_timestamp,
                                             tracked_order.client_order_id,
                                             tracked_order.trading_pair,
                                             tracked_order.trade_type,
                                             tracked_order.order_type,
                                             executed_price,
                                             executed_amount_diff,
                                             self.c_get_fee(
                                                 tracked_order.base_asset,
                                                 tracked_order.quote_asset,
                                                 tracked_order.order_type,
                                                 tracked_order.trade_type,
                                                 executed_price,
                                                 executed_amount_diff
                                             )
                                         ))

                if order_state == "closed":
                    if order["size"] == order["filledSize"]:  # Order COMPLETED
                        tracked_order.last_state = "CLOSED"
                        self.logger().info(f"The {order_type}-{trade_type} "
                                           f"{client_order_id} has completed according to ftx order status API.")

                        if tracked_order.trade_type is TradeType.BUY:
                            self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                                 BuyOrderCompletedEvent(
                                                     self._current_timestamp,
                                                     tracked_order.client_order_id,
                                                     tracked_order.base_asset,
                                                     tracked_order.quote_asset,
                                                     tracked_order.fee_asset or tracked_order.base_asset,
                                                     tracked_order.executed_amount_base,
                                                     tracked_order.executed_amount_quote,
                                                     tracked_order.fee_paid,
                                                     tracked_order.order_type))
                        elif tracked_order.trade_type is TradeType.SELL:
                            self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                                 SellOrderCompletedEvent(
                                                     self._current_timestamp,
                                                     tracked_order.client_order_id,
                                                     tracked_order.base_asset,
                                                     tracked_order.quote_asset,
                                                     tracked_order.fee_asset or tracked_order.base_asset,
                                                     tracked_order.executed_amount_base,
                                                     tracked_order.executed_amount_quote,
                                                     tracked_order.fee_paid,
                                                     tracked_order.order_type))
                    else:  # Order PARTIAL-CANCEL or CANCEL
                        tracked_order.last_state = "CANCELLED"
                        self.logger().info(f"The {tracked_order.order_type}-{tracked_order.trade_type} "
                                           f"{client_order_id} has been cancelled according to ftx order status API.")
                        self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                             OrderCancelledEvent(
                                                 self._current_timestamp,
                                                 client_order_id
                                             ))

                    self.c_stop_tracking_order(client_order_id)

    async def _iter_user_stream_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unknown error. Retrying after 1 second.", exc_info=True)
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_stream_queue():
            try:
                channel = stream_message.get("channel")
                data = stream_message.get("data")
                event_type = stream_message.get("type")

                if (channel == "orders") and (event_type == "update"):  # Updates track order status
                    order_status = data["status"]
                    order_id = str(data["id"])

                    tracked_order = None
                    for o in self._in_flight_orders.values():
                        if o.exchange_order_id == order_id:
                            tracked_order = o
                            break

                    if tracked_order is None:
                        continue

                    order_type_description = tracked_order.order_type_description

                    execute_amount_diff = s_decimal_0

                    if data["avgFillPrice"]:
                        execute_price = Decimal(data["avgFillPrice"])

                        remaining_size = Decimal(str(data["remainingSize"]))

                        new_confirmed_amount = Decimal(tracked_order.amount - remaining_size)
                        execute_amount_diff = Decimal(new_confirmed_amount - tracked_order.executed_amount_base)
                        tracked_order.executed_amount_base = new_confirmed_amount
                        tracked_order.executed_amount_quote += Decimal(execute_amount_diff * execute_price)

                        if order_status == "closed":  # FILL(COMPLETE)
                            tracked_order.last_state = "CLOSED"
                            if tracked_order.trade_type is TradeType.BUY:
                                self.logger().info(f"The LIMIT_BUY order {tracked_order.client_order_id} has completed "
                                                   f"according to order delta websocket API.")
                                self.c_trigger_event(self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                                                     BuyOrderCompletedEvent(
                                                         self._current_timestamp,
                                                         tracked_order.client_order_id,
                                                         tracked_order.base_asset,
                                                         tracked_order.quote_asset,
                                                         tracked_order.fee_asset or tracked_order.quote_asset,
                                                         tracked_order.executed_amount_base,
                                                         tracked_order.executed_amount_quote,
                                                         tracked_order.fee_paid,
                                                         tracked_order.order_type
                                                     ))
                            elif tracked_order.trade_type is TradeType.SELL:
                                self.logger().info(f"The LIMIT_SELL order {tracked_order.client_order_id} has completed "
                                                   f"according to Order Delta WebSocket API.")
                                self.c_trigger_event(self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                                                     SellOrderCompletedEvent(
                                                         self._current_timestamp,
                                                         tracked_order.client_order_id,
                                                         tracked_order.base_asset,
                                                         tracked_order.quote_asset,
                                                         tracked_order.fee_asset or tracked_order.quote_asset,
                                                         tracked_order.executed_amount_base,
                                                         tracked_order.executed_amount_quote,
                                                         tracked_order.fee_paid,
                                                         tracked_order.order_type
                                                     ))
                            self.c_stop_tracking_order(tracked_order.client_order_id)
                            continue

                elif (channel == "fills") and (event_type == "update"):
                    order_id = str(data["orderId"])

                    tracked_order = None
                    for o in self._in_flight_orders.values():
                        if o.exchange_order_id == order_id:
                            tracked_order = o
                            break

                    if tracked_order is None:
                        continue

                    order_type_description = tracked_order.order_type_description

                    execute_amount = data["size"]
                    execute_price = data["price"]

                    tracked_order.executed_amount_base += Decimal(execute_amount)
                    tracked_order.executed_amount_quote += Decimal(execute_amount * execute_price)

                    self.logger().info(f"Filled {execute_amount} out of {tracked_order.amount} of the "
                                           f"{order_type_description} order {tracked_order.client_order_id}.")
                    self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                             OrderFilledEvent(
                                                 self._current_timestamp,
                                                 tracked_order.client_order_id,
                                                 tracked_order.trading_pair,
                                                 tracked_order.trade_type,
                                                 tracked_order.order_type,
                                                 execute_price,
                                                 execute_amount,
                                                 self.c_get_fee(
                                                     tracked_order.base_asset,
                                                     tracked_order.quote_asset,
                                                     tracked_order.order_type,
                                                     tracked_order.trade_type,
                                                     execute_price,
                                                     execute_amount
                                                 )
                                             ))
                    continue
                else:
                    # Ignores all other user stream message types
                    continue

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self._current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while polling updates.",
                                      exc_info=True,
                                      app_warning_msg=f"Could not fetch updates from ftx. "
                                                      f"Check API key and network connection.")
                await asyncio.sleep(5.0)

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60 * 5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rule updates.",
                                      exc_info=True,
                                      app_warning_msg=f"Could not fetch updates from ftx. "
                                                      f"Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def get_deposit_address(self, currency: str) -> str:
        path_url = f"/addresses/{currency}"

        deposit_result = await self._api_request("GET", path_url=path_url)
        return deposit_result.get("cryptoAddress")

    async def get_deposit_info(self, asset: str) -> DepositInfo:
        return DepositInfo(await self.get_deposit_address(asset))

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    cdef c_start_tracking_order(self,
                                str order_id,
                                str exchange_order_id,
                                str trading_pair,
                                object order_type,
                                object trade_type,
                                object price,
                                object amount):
        self._in_flight_orders[order_id] = FtxInFlightOrder(
            order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount
        )

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    cdef c_did_timeout_tx(self, str tracking_id):
        self.c_trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                             MarketTransactionFailureEvent(self._current_timestamp, tracking_id))

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_price_increment)

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=0.0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object quantized_amount = MarketBase.c_quantize_order_amount(self, trading_pair, amount)

        global s_decimal_0
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        if quantized_amount < trading_rule.min_order_value:
            return s_decimal_0

        return quantized_amount

    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Optional[Decimal]) -> Dict[str, Any]:

        path_url = "/orders"

        body = {}
        if order_type is OrderType.LIMIT:
            body = {
                "market": str(trading_pair),
                "side": "buy" if is_buy else "sell",
                "price": price,
                "size": amount,
                "type": "limit",
                "reduceOnly": False,
                "ioc": False,
                "postOnly": False,
                "clientId": str(order_id),
            }
        elif order_type is OrderType.MARKET:
            body = {
                "market": str(trading_pair),
                "side": "buy" if is_buy else "sell",
                "price": None,
                "type": "market",
                "size": amount,
                "reduceOnly": False,
                "ioc": False,
                "postOnly": False,
                "clientId": str(order_id),
            }

        api_response = await self._api_request("POST", path_url=path_url, body=body)

        return api_response

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            double quote_amount
            object decimal_amount
            object decimal_price
            str exchange_order_id
            object tracked_order

        decimal_amount = self.c_quantize_order_amount(trading_pair, amount)
        decimal_price = (self.c_quantize_order_price(trading_pair, price)
                         if order_type is OrderType.LIMIT
                         else s_decimal_0)

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            order_result = None
            self.c_start_tracking_order(
                order_id,
                None,
                trading_pair,
                order_type,
                TradeType.BUY,
                decimal_price,
                decimal_amount
            )
            if order_type is OrderType.LIMIT:
                order_result = await self.place_order(order_id,
                                                      trading_pair,
                                                      decimal_amount,
                                                      True,
                                                      order_type,
                                                      decimal_price)
            elif order_type is OrderType.MARKET:
                order_result = await self.place_order(order_id,
                                                      trading_pair,
                                                      decimal_amount,
                                                      True,
                                                      order_type,
                                                      None)

            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = str(order_result["result"]["id"])

            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None and exchange_order_id:
                tracked_order.update_exchange_order_id(exchange_order_id)
                order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
                self.logger().info(f"Created {order_type_str} buy order {order_id} for "
                                   f"{decimal_amount} {trading_pair}")
                self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                     BuyOrderCreatedEvent(
                                         self._current_timestamp,
                                         order_type,
                                         trading_pair,
                                         decimal_amount,
                                         decimal_price,
                                         order_id
                                     ))

        except asyncio.CancelledError:
            raise
        except Exception:
            tracked_order = self._in_flight_orders.get(order_id)
            tracked_order.last_state = "FAILURE"
            self.c_stop_tracking_order(order_id)
            order_type_str = "LIMIT" if order_type is OrderType.LIMIT else "MARKET"
            self.logger().error(
                f"Error submitting buy {order_type_str} order to ftx for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(
                                     self._current_timestamp,
                                     order_id,
                                     order_type
                                 ))

    cdef str c_buy(self,
                   str trading_pair,
                   object amount,
                   object order_type=OrderType.LIMIT,
                   object price=NaN,
                   dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str order_id = str(f"buy-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType = OrderType.LIMIT,
                           price: Optional[Decimal] = NaN):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            double quote_amount
            object decimal_amount
            object decimal_price
            str exchange_order_id
            object tracked_order

        decimal_amount = self.c_quantize_order_amount(trading_pair, amount)
        decimal_price = (self.c_quantize_order_price(trading_pair, price)
                         if order_type is OrderType.LIMIT
                         else s_decimal_0)

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}")

        try:
            order_result = None

            self.c_start_tracking_order(
                order_id,
                None,
                trading_pair,
                order_type,
                TradeType.SELL,
                decimal_price,
                decimal_amount
            )

            if order_type is OrderType.LIMIT:
                order_result = await self.place_order(order_id,
                                                      trading_pair,
                                                      decimal_amount,
                                                      False,
                                                      order_type,
                                                      decimal_price)
            elif order_type is OrderType.MARKET:
                decimal_price = self.c_get_price(trading_pair, False)
                order_result = await self.place_order(order_id,
                                                      trading_pair,
                                                      decimal_amount,
                                                      False,
                                                      order_type,
                                                      decimal_price)
            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = str(order_result["result"]["id"])
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None and exchange_order_id:
                tracked_order.update_exchange_order_id(exchange_order_id)
                order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
                self.logger().info(f"Created {order_type_str} sell order {order_id} for "
                                   f"{decimal_amount} {trading_pair}.")
                self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                     SellOrderCreatedEvent(
                                         self._current_timestamp,
                                         order_type,
                                         trading_pair,
                                         decimal_amount,
                                         decimal_price,
                                         order_id
                                     ))
        except asyncio.CancelledError:
            raise
        except Exception:
            tracked_order = self._in_flight_orders.get(order_id)
            tracked_order.last_state = "FAILURE"
            self.c_stop_tracking_order(order_id)
            order_type_str = "LIMIT" if order_type is OrderType.LIMIT else "MARKET"
            self.logger().network(
                f"Error submitting sell {order_type_str} order to ftx for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to ftx. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_id, order_type))

    cdef str c_sell(self,
                    str trading_pair,
                    object amount,
                    object order_type=OrderType.MARKET,
                    object price=0.0,
                    dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str order_id = str(f"sell-{trading_pair}-{tracking_nonce}")

        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_cancel(self, trading_pair: str, order_id: str):
        tracked_order = self._in_flight_orders.get(order_id)

        if tracked_order is None:
            self.logger().error(f"The order {order_id} is not tracked. ")
            raise ValueError
        path_url = f"/orders/{tracked_order.exchange_order_id}"

        cancel_result = await self._api_request("DELETE", path_url=path_url)

        if cancel_result["success"] == True:
            self.logger().info(f"Successfully cancelled order {order_id}.")
            tracked_order.last_state = "CANCELLED"
            self.c_stop_tracking_order(order_id)
            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                 OrderCancelledEvent(self._current_timestamp, order_id))
            return order_id
        if (cancel_result["success"] == False) and (cancel_result['error'] == 'Order already closed'):
            tracked_order.last_state = "CANCELLED"
            self.c_stop_tracking_order(order_id)
            return order_id

        self.logger().network(
            f"Failed to cancel order {order_id}.",
            exc_info=True,
            app_warning_msg=f"Failed to cancel the order {order_id} on ftx. "
                            f"Check API key and network connection."
        )
        return None

    cdef c_cancel(self, str trading_pair, str order_id):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [order for order in self._in_flight_orders.values() if not order.is_done]

        tasks = [self.execute_cancel(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellation = []

        try:
            async with timeout(timeout_seconds):
                api_responses = await safe_gather(*tasks, return_exceptions=True)
                for order_id in api_responses:
                    if order_id:
                        order_id_set.remove(order_id)
                        successful_cancellation.append(CancellationResult(order_id, True))
        except Exception:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                app_warning_msg="Failed to cancel order on ftx. Check API key and network connection."
            )

        failed_cancellation = [CancellationResult(oid, False) for oid in order_id_set]
        return successful_cancellation + failed_cancellation

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def _api_request(self,
                           http_method: str,
                           path_url: str = None,
                           params: Dict[str, any] = None,
                           body: Dict[str, any] = None) -> Dict[str, Any]:
        assert path_url is not None

        url = f"{self.FTX_API_ENDPOINT}{path_url}"

        headers = self.ftx_auth.generate_auth_dict(http_method, url, params, body)

        if http_method == 'POST':
            res = requests.post(url, json=body, headers=headers)
            res_body = res.text
            return simplejson.loads(res_body, parse_float=Decimal)
        else:
            client = await self._http_client()
            async with client.request(http_method,
                                      url=url,
                                      headers=headers,
                                      data=body,
                                      timeout=self.API_CALL_TIMEOUT) as response:
                res_body = await response.text()
                data = simplejson.loads(res_body, parse_float=Decimal)
                if http_method == 'DELETE':
                    return data
                if response.status not in [200, 201]:  # HTTP Response code of 20X generally means it is successful
                    raise IOError(f"Error fetching response from {http_method}-{url}. HTTP Status Code {response.status}: "
                                  f"{data}")
                return data

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request("GET", path_url="/subaccounts")
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._status_polling_task = self._user_stream_tracker_task = \
            self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()

    async def start_network(self):
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

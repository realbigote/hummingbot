import re
import time
import asyncio
import aiohttp
import logging
import pandas as pd
import simplejson
from decimal import Decimal
from libc.stdint cimport int64_t
from threading import Lock
from async_timeout import timeout
from typing import Optional, List, Dict, Any, AsyncIterable, Tuple


from hummingbot.core.clock cimport Clock
from hummingbot.market.market_base import (
    MarketBase,
    NaN
)
from hummingbot.logger import HummingbotLogger
from hummingbot.market.deposit_info import DepositInfo
from hummingbot.market.trading_rule cimport TradingRule
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.market.blocktane.blocktane_auth import BlocktaneAuth
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.event.events import (
    MarketEvent,
    TradeFee,
    OrderType,
    OrderFilledEvent,
    TradeType,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent, OrderCancelledEvent, MarketTransactionFailureEvent,
    MarketOrderFailureEvent, SellOrderCreatedEvent, BuyOrderCreatedEvent
)
from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.market.blocktane.blocktane_in_flight_order import BlocktaneInFlightOrder
from hummingbot.market.blocktane.blocktane_order_book_tracker import BlocktaneOrderBookTracker
from hummingbot.market.blocktane.blocktane_user_stream_tracker import BlocktaneUserStreamTracker
from hummingbot.market.blocktane.blocktane_api_order_book_data_source import BlocktaneAPIOrderBookDataSource

bm_logger = None
s_decimal_0 = Decimal(0)
TRADING_PAIR_SPLITTER = re.compile(r"^(\w+)(BTC|btc|ETH|eth|BRL|brl|PAX|pax)$")

cdef class BlocktaneMarketTransactionTracker(TransactionTracker):
    cdef:
        BlocktaneMarket _owner

    def __init__(self, owner: BlocktaneMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class BlocktaneMarket(MarketBase):
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

    BLOCKTANE_API_ENDPOINT = "https://bolsa.tokamaktech.net/api/v2/xt"

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global bm_logger
        if bm_logger is None:
            bm_logger = logging.getLogger(__name__)
        return bm_logger

    def __init__(self,
                 blocktane_api_key: str,
                 blocktane_secret_key: str,
                 poll_interval: float = 5.0,
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):
        super().__init__()
        self._account_id = ""
        self._account_available_balances = {}
        self._account_balances = {}
        self._blocktane_auth = BlocktaneAuth(blocktane_api_key, blocktane_secret_key)
        self._data_source_type = order_book_tracker_data_source_type
        self._ev_loop = asyncio.get_event_loop()
        self._in_flight_orders = {}
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._order_book_tracker = BlocktaneOrderBookTracker(
            data_source_type=order_book_tracker_data_source_type,
            trading_pairs=trading_pairs
        )
        self._order_not_found_records = {}
        self._order_tracker_task = None
        self._poll_notifier = asyncio.Event()
        self._poll_interval = poll_interval
        self._shared_client = None
        self._status_polling_task = None
        self._trading_required = trading_required
        self._trading_rules = {}
        self._trading_rules_polling_task = None
        self._tx_tracker = BlocktaneMarketTransactionTracker(self)
        self._user_stream_event_listener_task = None
        self._user_stream_tracker = BlocktaneUserStreamTracker(blocktane_auth=self._blocktane_auth, trading_pairs=trading_pairs)
        self._user_stream_tracker_task = None
        self._check_network_interval = 60.0

    @staticmethod
    def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
        try:
            if ('/' in trading_pair):
                m = trading_pair.split('/')
                return m[0], m[1]
            else:
                m = TRADING_PAIR_SPLITTER.match(trading_pair)
                return m.group(1), m.group(2)
        # Exceptions are now logged as warnings in trading pair fetcher
        except Exception as e:
            return None

    @staticmethod
    def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
        if BlocktaneMarket.split_trading_pair(exchange_trading_pair) is None:
            return None
        # Blocktane does not split BASEQUOTE (fthusd)
        base_asset, quote_asset = BlocktaneMarket.split_trading_pair(exchange_trading_pair)
        return f"{base_asset}-{quote_asset}".upper()
    
    @staticmethod
    def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
        return hb_trading_pair.lower().replace("-","")

    @property
    def name(self) -> str:
        return "blocktane"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def blocktane_auth(self) -> BlocktaneAuth:
        return self._blocktane_auth

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
            key: BlocktaneInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await BlocktaneAPIOrderBookDataSource.get_active_exchange_markets()

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
        # Fee info from https://bolsa.tokamaktech.net/api/v2/xt/public/trading_fees
        cdef:
            object maker_fee = Decimal(0.002)
            object taker_fee = Decimal(0.002)
        if order_type is OrderType.LIMIT and fee_overrides_config_map["blocktane_maker_fee"].value is not None:
            return TradeFee(percent=fee_overrides_config_map["blocktane_maker_fee"].value)
        if order_type is OrderType.MARKET and fee_overrides_config_map["blocktane_taker_fee"].value is not None:
            return TradeFee(percent=fee_overrides_config_map["blocktane_taker_fee"].value)

        return TradeFee(percent=maker_fee if order_type is OrderType.LIMIT else taker_fee)

    async def _update_balances(self):
        cdef:
            dict account_info
            list balances
            str asset_name
            set local_asset_names = set(self._account_balances.keys())
            set remote_asset_names = set()
            set asset_names_to_remove

        path_url = "/account/balances"
        account_balances = await self._api_request("GET", path_url=path_url)

        for balance_entry in account_balances:
            asset_name = balance_entry["currency"]
            available_balance = Decimal(balance_entry["balance"])
            total_balance = available_balance + Decimal(balance_entry["locked"]) 
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _format_trading_rules(self, market_dict: Dict[str, Dict[str, Any]]) -> List[TradingRule]:
        cdef:
            list trading_rules = []

        for pair, info in market_dict.items():
            try:
                trading_rules.append(
                    TradingRule(trading_pair=info["id"],
                                min_order_size=Decimal(info["min_amount"]),
                                min_price_increment=Decimal(f"1e-{info['price_precision']}"),
                                min_quote_amount_increment=Decimal(f"1e-{info['amount_precision']}"),
                                min_base_amount_increment=Decimal(f"1e-{info['amount_precision']}")
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {info}. Skipping.", exc_info=True)
        return trading_rules

    async def _update_trading_rules(self):
        cdef:
            # The poll interval for withdraw rules is 60 seconds.
            int64_t last_tick = <int64_t> (self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t> (self._current_timestamp / 60.0)
        if current_tick > last_tick or len(self._trading_rules) <= 0:
            market_path_url = "/public/markets"
            ticker_path_url = "/public/markets/tickers"

            market_list = await self._api_request("GET", path_url=market_path_url)

            ticker_list = await self._api_request("GET", path_url=ticker_path_url)
            ticker_data = {symbol: item['ticker'] for symbol, item in ticker_list.items()}

            result_list = {
                market["id"]:{**market, **ticker_data[market["id"]]}
                for market in market_list
                if market["id"] in ticker_data
            }

            trading_rules_list = self._format_trading_rules(result_list)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def list_orders(self) -> List[Any]:
        """
        Only a list of all currently open orders(does not include filled orders)
        :returns json response
        i.e.
        Result = [
            {
            id: 160,
            side: "sell",
            ord_type: "limit",
            price: "1.0",
            avg_price: "0.0",
            state: "wait",
            market: "fthusd",
            created_at: "2020-03-16T17:56:13+01:00",
            updated_at: "2020-03-16T17:56:13+01:00",
            origin_volume: "11.0",
            remaining_volume: "11.0",
            executed_volume: "0.0",
            trades_count: 0
            }
        ]

        """
        path_url = "/market/orders?state=wait"

        result = await self._api_request("GET", path_url=path_url)
        return result

    async def get_order(self, uuid: str) -> Dict[str, Any]:
        # Used to retrieve a single order by uuid
        path_url = f"/market/orders/{uuid}"

        result = await self._api_request("GET", path_url=path_url)
        return result

    async def _update_order_status(self):
        cdef:
            # This is intended to be a backup measure to close straggler orders, in case Blocktane's user stream events
            # are not capturing the updates as intended. Also handles filled events that are not captured by
            # _user_stream_event_listener
            # The poll interval for order status is 10 seconds.
            int64_t last_tick = <int64_t>(self._last_poll_timestamp / self.UPDATE_ORDERS_INTERVAL)
            int64_t current_tick = <int64_t>(self._current_timestamp / self.UPDATE_ORDERS_INTERVAL)

        try:
            if current_tick > last_tick and len(self._in_flight_orders) > 0:

                tracked_orders = list(self._in_flight_orders.values())
                open_orders = await self.list_orders()
                open_orders = dict((entry["id"], entry) for entry in open_orders)

                for tracked_order in tracked_orders:
                    exchange_order_id = await tracked_order.get_exchange_order_id()
                    client_order_id = tracked_order.client_order_id
                    order = open_orders.get(int(exchange_order_id))

                    # Do nothing, if the order has already been cancelled or has failed
                    if client_order_id not in self._in_flight_orders:
                        continue

                    if order is None:  # Handles order that are currently tracked but no longer open in exchange
                        self._order_not_found_records[client_order_id] = \
                            self._order_not_found_records.get(client_order_id, 0) + 1

                        if self._order_not_found_records[client_order_id] < self.ORDER_NOT_EXIST_CONFIRMATION_COUNT:
                            # Wait until the order not found error have repeated for a few times before actually treating
                            # it as a fail. See: https://github.com/CoinAlpha/hummingbot/issues/601
                            continue
                        tracked_order.last_state = "done"
                        self.c_trigger_event(
                            self.MARKET_ORDER_FAILURE_EVENT_TAG,
                            MarketOrderFailureEvent(self._current_timestamp,
                                                    client_order_id,
                                                    tracked_order.order_type)
                        )
                        self.c_stop_tracking_order(client_order_id)
                        self.logger().network(
                            f"Error fetching status update for the order {client_order_id}: "
                            f"{tracked_order}",
                            app_warning_msg=f"Could not fetch updates for the order {client_order_id}. "
                                            f"Check API key and network connection."
                        )
                        continue

                    order_state = order["state"]
                    order_type = "LIMIT" if tracked_order.order_type is OrderType.LIMIT else "MARKET"
                    trade_type = "BUY" if tracked_order.trade_type is TradeType.BUY else "SELL"

                    order_type_description = tracked_order.order_type
                    executed_amount_diff = s_decimal_0
                    avg_price = Decimal(order["avg_price"])
                    new_confirmed_amount = Decimal(order["executed_volume"])
                    executed_amount_base_diff = new_confirmed_amount - tracked_order.executed_amount_quote
                    if executed_amount_base_diff > s_decimal_0:
                        self.logger().info(f"Updated order status with fill from polling _update_order_status: {simplejson.dumps(order)}")
                        new_confirmed_quote_amount = new_confirmed_amount * avg_price
                        executed_amount_quote_diff = new_confirmed_quote_amount - tracked_order.executed_amount_quote
                        executed_price = executed_amount_quote_diff / executed_amount_base_diff

                        tracked_order.executed_amount_base = new_confirmed_amount
                        tracked_order.executed_amount_quote = new_confirmed_quote_amount
                        self.logger().info(f"Filled {executed_amount_base_diff} out of {tracked_order.amount} of the "
                                                f"{order_type} order {tracked_order.client_order_id}.")
                        self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                            OrderFilledEvent(
                                                self._current_timestamp,
                                                tracked_order.client_order_id,
                                                tracked_order.trading_pair,
                                                tracked_order.trade_type,
                                                tracked_order.order_type,
                                                executed_price,
                                                executed_amount_base_diff,
                                                self.c_get_fee(
                                                    tracked_order.base_asset,
                                                    tracked_order.quote_asset,
                                                    tracked_order.order_type,
                                                    tracked_order.trade_type,
                                                    executed_price,
                                                    executed_amount_base_diff
                                                )
                                            ))

                    if order_state == "done":
                        tracked_order.last_state = "done"
                        self.logger().info(f"The {order_type}-{trade_type} "
                                        f"{client_order_id} has completed according to Blocktane order status API.")

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

                    if order_state == "cancel":
                        tracked_order.last_state = "cancel"
                        self.logger().info(f"The {tracked_order.order_type}-{tracked_order.trade_type} "
                                        f"{client_order_id} has been cancelled according to Blocktane order status API.")
                        self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                                OrderCancelledEvent(
                                                    self._current_timestamp,
                                                    client_order_id
                                                ))

                        self.c_stop_tracking_order(client_order_id)
        except Exception as e:
            self.logger().error("Update Order Status Error: " + str(e) + " " + str(e.__cause__))

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
                if stream_message.get('order'): # Updates tracked orders
                    order = stream_message.get('order')
                    order_status = order["state"]
                    order_id = order["id"] #temporary, perhaps

                    tracked_order = None
                    for o in self._in_flight_orders.values():
                        if o.exchange_order_id is not None:
                            if int(o.exchange_order_id) == int(order_id):
                                tracked_order = o
                                break

                    if tracked_order is None:
                        self.logger().debug(f"Unrecognized order ID from user stream: {order_id}.")
                        continue

                    order_type = tracked_order.order_type
                    executed_amount_diff = s_decimal_0
                    avg_price = Decimal(order["avg_price"])
                    new_confirmed_amount = Decimal(order["executed_volume"])
                    executed_amount_base_diff = new_confirmed_amount - tracked_order.executed_amount_quote
                    if executed_amount_base_diff > s_decimal_0:
                        self.logger().info(f"Updated order status with fill from streaming _user_stream_event_listener: {simplejson.dumps(stream_message)}")
                        new_confirmed_quote_amount = new_confirmed_amount * avg_price
                        executed_amount_quote_diff = new_confirmed_quote_amount - tracked_order.executed_amount_quote
                        executed_price = executed_amount_quote_diff / executed_amount_base_diff

                        tracked_order.executed_amount_base = new_confirmed_amount
                        tracked_order.executed_amount_quote = new_confirmed_quote_amount
                        tracked_order.last_state = order_status
                        self.logger().info(f"Filled {executed_amount_base_diff} out of {tracked_order.amount} of the "
                                                f"{order_type} order {tracked_order.client_order_id}.")
                        self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG,
                                                OrderFilledEvent(
                                                    self._current_timestamp,
                                                    tracked_order.client_order_id,
                                                    tracked_order.trading_pair,
                                                    tracked_order.trade_type,
                                                    tracked_order.order_type,
                                                    executed_price,
                                                    executed_amount_base_diff,
                                                    self.c_get_fee(
                                                        tracked_order.base_asset,
                                                        tracked_order.quote_asset,
                                                        tracked_order.order_type,
                                                        tracked_order.trade_type,
                                                        executed_price,
                                                        executed_amount_base_diff
                                                    )
                                                ))

                    if order_status == "done":  # FILL(COMPLETE)
                        # trade_type = TradeType.BUY if content["OT"] == "LIMIT_BUY" else TradeType.SELL
                        tracked_order.last_state = "done"
                        if tracked_order.trade_type is TradeType.BUY:
                            self.logger().info(f"The LIMIT_BUY order {tracked_order.client_order_id} has completed "
                                                f"according to Blocktane websocket API.")
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
                                                f"according to Blocktane WebSocket API.")
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

                    if order_status == "cancel":  # CANCEL
                        tracked_order.last_state = "cancel"
                        self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                                OrderCancelledEvent(self._current_timestamp,
                                                                    tracked_order.client_order_id))
                        self.c_stop_tracking_order(tracked_order.client_order_id)
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
                                      app_warning_msg=f"Could not fetch updates from Blocktane. "
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
                                      app_warning_msg=f"Could not fetch updates from Blocktane. "
                                                      f"Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def get_order(self, client_order_id: str) -> Dict[str, Any]:
        order = self._in_flight_orders.get(client_order_id)
        exchange_order_id = await order.get_exchange_order_id()
        path_url = f"/market/orders/{exchange_order_id}"
        result = await self._api_request("GET", path_url=path_url)
        return result

    async def get_deposit_address(self, currency: str) -> str:
        path_url = f"/account/deposit_address/{currency}"

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
        self._in_flight_orders[order_id] = BlocktaneInFlightOrder(
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
                          price: Decimal) -> Dict[str, Any]:
        """
        Expected response:
        {
            "avg_price": "0.0",
            "created_at": "2020-03-12T17:01:56+01:00",
            "executed_volume": "0.0",
            "id": 10440269,
            "market": "ethusd",
            "ord_type": "limit",
            "origin_volume": "31.0",
            "price": "160.82",
            "remaining_volume": "31.0",
            "side": "buy",
            "state": "pending",
            "trades_count": 0,
            "updated_at": "2020-03-12T17:01:56+01:00"
        }
        """

        path_url = "/market/orders"

        params = {}
        if order_type is OrderType.LIMIT:  # Blocktane supports CEILING_LIMIT
            params = {
                "market": str(trading_pair),
                "side": "buy" if is_buy else "sell",
                "volume": f"{amount:f}",
                "ord_type": "limit",
                "price": f"{price:f}"
            }
        elif order_type is OrderType.MARKET:
            params = {
                "market": str(trading_pair),
                "side": "buy" if is_buy else "sell",
                "volume": str(amount),
                "ord_type": "market"
            }
        try:
            api_response = await self._api_request("POST", path_url=path_url, params=params)
            return api_response
        except Exception:
            raise Exception

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
                while order_result is None:
                    continue                        
            elif order_type is OrderType.MARKET:
                decimal_price = self.c_get_price(trading_pair, True)
                order_result = await self.place_order(order_id,
                                                      trading_pair,
                                                      decimal_amount,
                                                      True,
                                                      order_type,
                                                      decimal_price)

            else:
                raise ValueError(f"Invalid OrderType {order_type}. Aborting.")

            exchange_order_id = str(order_result["id"])

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
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Blocktane for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to Blocktane. Check API key and network connection."
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
                while order_result is None:
                    continue
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

            exchange_order_id = str(order_result["id"])
            
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
                f"Error submitting sell {order_type_str} order to Blocktane for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price if order_type is OrderType.LIMIT else ''}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to Blocktane. Check API key and network connection."
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
        try:
            tracked_order = self._in_flight_orders.get(order_id)

            if tracked_order is None:
                self.logger().error(f"The order {order_id} is not tracked. ")
                raise ValueError
            path_url = f"/market/orders/{tracked_order.exchange_order_id}/cancel"

            cancel_result = await self._api_request("POST", path_url=path_url)
            while cancel_result is None:
                continue
            self.logger().info(f"Requested cancel of order {order_id}.")
            return order_id
        except asyncio.CancelledError:
            raise
        except Exception as err:
            if "NOT_FOUND" in str(err):
                # The order was never there to begin with. So cancelling it is a no-op but semantically successful.
                self.logger().info(f"The order {order_id} does not exist on Blocktane. Marking as cancelled.")
                self.c_stop_tracking_order(order_id)
                self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                     OrderCancelledEvent(self._current_timestamp, order_id))
                return order_id

            self.logger().network(
                f"Failed to cancel order {order_id}: {str(err)}.",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Blocktane. "
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
                for res in api_responses:
                    order_id_set.remove(res)
                    successful_cancellation.append(CancellationResult(res, True))
        except Exception:
            self.logger().network(
                f"Unexpected error cancelling orders.",
                app_warning_msg="Failed to cancel order on Blocktane. Check API key and network connection."
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
                           body: Dict[str, any] = None,
                           subaccount_id: str = '') -> Dict[str, Any]:
        assert path_url is not None
        url = f"{self.BLOCKTANE_API_ENDPOINT}{path_url}"

        content_type = "application/json" if http_method == "post" else "application/x-www-form-urlencoded"
        headers = {"Content-Type": content_type}
        headers = self.blocktane_auth.generate_auth_dict()

        client = await self._http_client()
        async with client.request(http_method,
                                  url=url,
                                  headers=headers,
                                  params=params,
                                  data=body,
                                  timeout=self.API_CALL_TIMEOUT) as response:
                                  
            data = await response.json()
            if response.status not in [200, 201]:  # HTTP Response code of 20X generally means it is successful
                raise IOError(f"Error fetching response from {http_method}-{url}. HTTP Status Code {response.status}: "
                              f"{data}")
            return data

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request("GET", path_url="/public/health/alive")
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def _stop_network(self):
        if self._order_tracker_task is not None:
            self._order_tracker_task.cancel()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._order_tracker_task = self._status_polling_task = self._user_stream_tracker_task = \
            self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()

    async def start_network(self):
        if self._order_tracker_task is not None:
            self._stop_network()

        self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

import aiohttp
import asyncio
import binascii
import json
import time
import uuid
import traceback
import urllib
import hashlib
from typing import (
    Any,
    Dict,
    List,
    Optional
)
import math
import logging
from decimal import *
from libc.stdint cimport int64_t

from dydx.client import Client as DYDXClient
import dydx.constants as consts
import dydx.util as utils

from hummingbot.client.config.fee_overrides_config_map import fee_overrides_config_map
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.event.event_listener cimport EventListener
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.wallet.ethereum.web3_wallet import Web3Wallet
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.connector.exchange.dydx.dydx_auth import DydxAuth
from hummingbot.connector.exchange.dydx.dydx_order_book_tracker import DydxOrderBookTracker
from hummingbot.connector.exchange.dydx.dydx_api_order_book_data_source import DydxAPIOrderBookDataSource
from hummingbot.connector.exchange.dydx.dydx_api_token_configuration_data_source import DydxAPITokenConfigurationDataSource
from hummingbot.connector.exchange.dydx.dydx_user_stream_tracker import DydxUserStreamTracker
from hummingbot.connector.exchange.dydx.dydx_utils import hash_order_id
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
)
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderCancelledEvent,
    OrderExpiredEvent,
    OrderFilledEvent,
    MarketOrderFailureEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    TradeType,
    OrderType,
    TradeFee,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.dydx.dydx_in_flight_order cimport DydxInFlightOrder
from hummingbot.connector.trading_rule cimport TradingRule
from hummingbot.core.utils.estimate_fee import estimate_fee
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

s_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("nan")


def num_d(amount):
    return abs(Decimal(amount).normalize().as_tuple().exponent)


def now():
    return int(time.time()) * 1000


BUY_ORDER_COMPLETED_EVENT = MarketEvent.BuyOrderCompleted.value
SELL_ORDER_COMPLETED_EVENT = MarketEvent.SellOrderCompleted.value
ORDER_CANCELLED_EVENT = MarketEvent.OrderCancelled.value
ORDER_EXPIRED_EVENT = MarketEvent.OrderExpired.value
ORDER_FILLED_EVENT = MarketEvent.OrderFilled.value
ORDER_FAILURE_EVENT = MarketEvent.OrderFailure.value
BUY_ORDER_CREATED_EVENT = MarketEvent.BuyOrderCreated.value
SELL_ORDER_CREATED_EVENT = MarketEvent.SellOrderCreated.value
API_CALL_TIMEOUT = 10.0

# ==========================================================

GET_ORDER_ROUTE = "v2/orders/"
MAINNET_API_REST_ENDPOINT = "https://api.dydx.exchange/"
MAINNET_WS_ENDPOINT = "wss://api.dydx.exchange/v1/ws"
#EXCHANGE_INFO_ROUTE = "api/v2/timestamp"
BALANCES_INFO_ROUTE = "v1/accounts/:wallet"
MARKETS_INFO_ROUTE = "v2/markets"
#TOKENS_INFO_ROUTE = "api/v2/exchange/tokens"
#NEXT_ORDER_ID = "api/v2/orderId"
ORDER_ROUTE = "v2/orders"
ORDER_CANCEL_ROUTE = "v2/orders"
#MAXIMUM_FILL_COUNT = 16
#UNRECOGNIZED_ORDER_DEBOUCE = 20  # seconds
MANUAL_AMOUNT_INC = Decimal('0.00000000001')

class LatchingEventResponder(EventListener):
    def __init__(self, callback : any, num_expected : int):
        super().__init__()
        self._callback = callback
        self._completed = asyncio.Event()
        self._num_remaining = num_expected

    def __call__(self, arg : any):
        if self._callback(arg):
            self._reduce()

    def _reduce(self):
        self._num_remaining -= 1
        if self._num_remaining <= 0:
            self._completed.set()

    async def wait_for_completion(self, timeout : float):
        try:
            await asyncio.wait_for(self._completed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return self._completed.is_set()

    def cancel_one(self):
        self._reduce()


cdef class DydxExchangeTransactionTracker(TransactionTracker):
    cdef:
        DydxExchange _owner

    def __init__(self, owner: DydxExchange):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class DydxExchange(ExchangeBase):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 wallet: Web3Wallet,
                 ethereum_rpc_url: str,
                 poll_interval: float = 10.0,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()

        self._real_time_balance_update = True

        self._dydx_auth = DydxAuth(wallet.address)
        self._token_configuration = DydxAPITokenConfigurationDataSource()

        self.API_REST_ENDPOINT = MAINNET_API_REST_ENDPOINT
        self.WS_ENDPOINT = MAINNET_WS_ENDPOINT
        self._order_book_tracker = DydxOrderBookTracker(
            trading_pairs=trading_pairs,
            rest_api_url=self.API_REST_ENDPOINT,
            websocket_url=self.WS_ENDPOINT,
            token_configuration = self._token_configuration
        )        
        self._user_stream_tracker = DydxUserStreamTracker(
            orderbook_tracker_data_source=self._order_book_tracker.data_source,
            dydx_auth=self._dydx_auth
        )
        self._user_stream_event_listener_task = None
        self._user_stream_tracker_task = None
        self._tx_tracker = DydxExchangeTransactionTracker(self)
        self._trading_required = trading_required
        self._poll_notifier = asyncio.Event()
        self._last_timestamp = 0
        self._poll_interval = poll_interval
        self._shared_client = None
        self._polling_update_task = None

        self._dydx_private_key = wallet.private_key
        self._dydx_node = ethereum_rpc_url
        self._dydx_client: DYDXClient = DYDXClient(private_key=self._dydx_private_key, node=self._dydx_node)
        # State
        self._lock = asyncio.Lock()
        self._trading_rules = {}
        self._pending_approval_tx_hashes = set()
        self._in_flight_orders = {}
        self._trading_pairs = trading_pairs
        self._fee_rules = {}
        self._order_id_lock = asyncio.Lock()
        self._fee_override = ("dydx_maker_fee_amount" in fee_overrides_config_map)

    @property
    def name(self) -> str:
        return "dydx"

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": len(self._order_book_tracker.order_books) > 0,
            "account_balances": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True,
        }

    @property
    def token_configuration(self) -> DydxAPITokenConfigurationDataSource:
        if not self._token_configuration:
            self._token_configuration = DydxAPITokenConfigurationDataSource.create()
        return self._token_configuration

    # ----------------------------------------
    # Markets & Order Books

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef dict order_books = self._order_book_tracker.order_books
        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    @property
    def limit_orders(self) -> List[LimitOrder]:
        cdef:
            list retval = []
            DydxInFlightOrder dydx_flight_order

        for in_flight_order in self._in_flight_orders.values():
            dydx_flight_order = in_flight_order
            if dydx_flight_order.order_type is OrderType.LIMIT:
                retval.append(dydx_flight_order.to_limit_order())
        return retval

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await DydxAPIOrderBookDataSource.get_active_exchange_markets()

    # ----------------------------------------
    # Account Balances

    cdef object c_get_balance(self, str currency):
        return self._account_balances[currency]

    cdef object c_get_available_balance(self, str currency):
        return self._account_available_balances[currency]

    # ==========================================================
    # Order Submission
    # ----------------------------------------------------------

    @property
    def in_flight_orders(self) -> Dict[str, DydxInFlightOrder]:
        return self._in_flight_orders

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER]

    async def place_order(self,
                          client_order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal) -> Dict[str, Any]:
                  
        order_side = "BUY" if is_buy else "SELL"
        base, quote = trading_pair.split('-')
        
        baseid, quoteid = self._token_configuration.get_tokenid(base), self._token_configuration.get_tokenid(quote)
        validSince = int(time.time()) - 3600
        
        order_details = self._token_configuration.sell_buy_amounts(baseid, quoteid, amount, price, order_side)
        post_only=False
        if order_type is OrderType.LIMIT_MAKER:
            post_only=True

        return self._dydx_client.place_orer(
          market=trading_pair,
          side=order_side,
          amount=order_details["amount"],
          price=order_details["price"],
          killOrFill=False,
          post_only=post_only,
        )

    async def execute_order(self, order_side, client_order_id, trading_pair, amount, order_type, price):
        """
        Completes the common tasks from execute_buy and execute_sell.  Quantizes the order's amount and price, and
        validates the order against the trading rules before placing this order.
        """
        # Quantize order
        amount = self.c_quantize_order_amount(trading_pair, amount)
        price = self.c_quantize_order_price(trading_pair, price)

        # Check trading rules
        if order_type == OrderType.LIMIT:
            trading_rule = self._trading_rules[f"{trading_pair}-limit"]
            if amount < trading_rule.min_order_size:
                amount = s_decimal_0
        elif order_type == OrderType.MARKET:
            trading_rule = self._trading_rules[f"{trading_pair}-market"]
        if order_type == OrderType.LIMIT and trading_rule.supports_limit_orders is False:
            raise ValueError("LIMIT orders are not supported")
        elif order_type == OrderType.MARKET and trading_rule.supports_market_orders is False:
            raise ValueError("MARKET orders are not supported")

        #if amount < trading_rule.min_order_size:
        #    raise ValueError(f"Order amount({str(amount)}) is less than the minimum allowable amount({str(trading_rule.min_order_size)})")
        if amount > trading_rule.max_order_size:
            raise ValueError(f"Order amount({str(amount)}) is greater than the maximum allowable amount({str(trading_rule.max_order_size)})")
        #if amount*price < trading_rule.min_notional_size:
        #    raise ValueError(f"Order notional value({str(amount*price)}) is less than the minimum allowable notional value for an order ({str(trading_rule.min_notional_size)})")

        try:
            created_at: int = int(time.time())
            in_flight_order = DydxInFlightOrder.from_dydx_order(self, order_side, client_order_id, created_at, None, trading_pair, price, amount)
            self.start_tracking(in_flight_order)

            try:
                creation_response = await self.place_order(client_order_id, trading_pair, amount, order_side is TradeType.BUY, order_type, price)
            except asyncio.exceptions.TimeoutError:
                # We timed out while placing this order. We may have successfully submitted the order, or we may have had connection
                # issues that prevented the submission from taking place. We'll assume that the order is live and let our order status 
                # updates mark this as cancelled if it doesn't actually exist.             
                return
                
            # Verify the response from the exchange
            if "order" not in creation_response.keys():
                raise Exception(creation_response['errors'][0]['msg'])

            status = creation_response["order"]["status"]
            if status not in ['PENDING', 'OPEN']:
                raise Exception(status)

            dydx_order_id = create_response["data"]["id"]
            in_flight_order.update_exchange_order_id(dydx_order_id)

            # Begin tracking order
            self.logger().info(
                f"Created {in_flight_order.description} order {client_order_id} for {amount} {trading_pair}.")

        except Exception as e:
            self.logger().warning(f"Error submitting {order_side.name} {order_type.name} order to dydx for "
                                  f"{amount} {trading_pair} at {price}.")
            self.logger().info(e)
            traceback.print_exc()

            # Stop tracking this order
            self.stop_tracking(client_order_id)
            self.c_trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), client_order_id, order_type))

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = Decimal('NaN')):
        try:
            await self.execute_order(TradeType.BUY, order_id, trading_pair, amount, order_type, price)

            self.c_trigger_event(BUY_ORDER_CREATED_EVENT,
                                 BuyOrderCreatedEvent(now(), order_type, trading_pair, Decimal(amount), Decimal(price), order_id))
        except ValueError as e:
            # Stop tracking this order
            self.stop_tracking(order_id)
            self.c_trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), order_id, order_type))
            raise e

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = Decimal('NaN')):
        try:
            await self.execute_order(TradeType.SELL, order_id, trading_pair, amount, order_type, price)
            self.c_trigger_event(SELL_ORDER_CREATED_EVENT,
                                 SellOrderCreatedEvent(now(), order_type, trading_pair, Decimal(amount), Decimal(price), order_id))
        except ValueError as e:
            # Stop tracking this order
            self.stop_tracking(order_id)
            self.c_trigger_event(ORDER_FAILURE_EVENT, MarketOrderFailureEvent(now(), order_id, order_type))
            raise e

    cdef str c_buy(self, str trading_pair, object amount, object order_type = OrderType.LIMIT, object price = 0.0,
                   dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = str(f"buy-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_buy(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    cdef str c_sell(self, str trading_pair, object amount, object order_type = OrderType.LIMIT, object price = 0.0,
                    dict kwargs = {}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = str(f"sell-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_sell(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    # ----------------------------------------
    # Cancellation

    async def cancel_order(self, client_order_id: str):
        in_flight_order = self._in_flight_orders.get(client_order_id)
        cancellation_event = OrderCancelledEvent(now(), client_order_id)
        exchange_order_id = in_flight_order.exchange_order_id

        if in_flight_order is None:
            self.c_trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
            return

        try:            
            res = await self._dydx_client.cancel_order(exchange_order_id)
            
            if "errors" in res:
                # TODO: Verify what happens if we try to cancel an order before it fully exists (we have a response from place order)
                # If this says that it doesn't exist, don't stop tracking this order until X time has passed
                if res["errors"][0]["msg"] == f"Order with specified id: {exchange_order_id} could not be found":
                # Order didn't exist on exchange, mark this as canceled
                    self.c_trigger_event(ORDER_CANCELLED_EVENT,cancellation_event)
                else:
                    raise Exception(f"Cancel order returned {res}")
            
            return True

        except Exception as e:
            self.logger().warning(f"Failed to cancel order {client_order_id}")
            self.logger().info(e)
            return False

    cdef c_cancel(self, str trading_pair, str client_order_id):
        safe_ensure_future(self.cancel_order(client_order_id))

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        cancellation_queue = self._in_flight_orders.copy()
        if len(cancellation_queue) == 0:
            return []

        order_status = {o.client_order_id: False for o in cancellation_queue.values()}
        for o, s in order_status.items():
            self.logger().info(o + ' ' + str(s))
        
        def set_cancellation_status(oce : OrderCancelledEvent):
            if oce.order_id in order_status:
                order_status[oce.order_id] = True
                return True
            return False
            
        cancel_verifier = LatchingEventResponder(set_cancellation_status, len(cancellation_queue))
        self.c_add_listener(ORDER_CANCELLED_EVENT, cancel_verifier)

        for order_id, in_flight in cancellation_queue.iteritems():
            try:            
                if not await self.cancel_order(order_id):
                    # this order did not exist on the exchange
                    cancel_verifier.cancel_one()
            except Exception:
                cancel_verifier.cancel_one()
        
        all_completed : bool = await cancel_verifier.wait_for_completion(timeout_seconds)
        self.c_remove_listener(ORDER_CANCELLED_EVENT, cancel_verifier)

        return [CancellationResult(order_id=order_id, success=success) for order_id, success in order_status.items()]

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        is_maker = order_type is OrderType.LIMIT
        market = f"{base_currency}-{quote_currency}".upper()
        if (market in self._fee_rules) and (not self._fee_override):
            fee_rule = self._fee_rules[market]
            if is_maker:
                return TradeFee(percent=fee_rule["maker"])
            else:
                trading_rule = self._trading_rules[f"{market}-limit"] # the small order threshold is the same as the min limit order
                if amount >= trading_rule.min_order_size:
                    return TradeFee(percent=fee_rule["largeTakerFee"])
                else:
                    return TradeFee(percent=fee_rule["smallTakerFee"])
        else:
            return estimate_fee("dydx", is_maker)

    # ==========================================================
    # Runtime
    # ----------------------------------------------------------

    async def start_network(self):
        await self.stop_network()
        await self._token_configuration._configure()
        self._order_book_tracker.start()
        if self._trading_required:
            exchange_info = await self.api_request("GET", MARKETS_INFO_ROUTE)

            tokens = set()
            for pair in self._trading_pairs:
                (base, quote) = self.split_trading_pair(pair)
                tokens.add(self.token_configuration.get_tokenid(base))
                tokens.add(self.token_configuration.get_tokenid(quote))
        self._polling_update_task = safe_ensure_future(self._polling_update())
        self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
        self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    async def stop_network(self):
        self._order_book_tracker.stop()
        self._pending_approval_tx_hashes.clear()
        self._polling_update_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None

    async def check_network(self) -> NetworkStatus:
        try:
            await self.api_request("GET", MARKETS_INFO_ROUTE)
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    # ----------------------------------------
    # State Management

    @property
    def tracking_states(self) -> Dict[str, any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, any]):
        for order_id, in_flight_repr in saved_states.iteritems():
            in_flight_json: Dict[Str, Any] = json.loads(in_flight_repr)
            self._in_flight_orders[order_id] = DydxInFlightOrder.from_json(self, in_flight_json)

    def start_tracking(self, in_flight_order):
        self._in_flight_orders[in_flight_order.client_order_id] = in_flight_order

    def stop_tracking(self, client_order_id):
        if client_order_id in self._in_flight_orders:
            del self._in_flight_orders[client_order_id]

    # ----------------------------------------
    # updates to orders and balances

    def _update_inflight_order(self, tracked_order: DydxInFlightOrder, event: Dict[str, Any]):
        issuable_events: List[MarketEvent] = tracked_order.update(event, self._dydx_client)

        # Issue relevent events
        for (market_event, new_amount, new_price, new_fee) in issuable_events:
            if market_event == MarketEvent.OrderCancelled:
                self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id}")
                self.stop_tracking(tracked_order.client_order_id)
                self.c_trigger_event(ORDER_CANCELLED_EVENT,
                                     OrderCancelledEvent(self._current_timestamp,
                                                         tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderFilled:
                self.c_trigger_event(ORDER_FILLED_EVENT,
                                     OrderFilledEvent(self._current_timestamp,
                                                      tracked_order.client_order_id,
                                                      tracked_order.trading_pair,
                                                      tracked_order.trade_type,
                                                      tracked_order.order_type,
                                                      new_price,
                                                      new_amount,
                                                      TradeFee(Decimal(0), [(tracked_order.fee_asset, new_fee)]),
                                                      tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderExpired:
                self.c_trigger_event(ORDER_EXPIRED_EVENT,
                                     OrderExpiredEvent(self._current_timestamp,
                                                       tracked_order.client_order_id))
            elif market_event == MarketEvent.OrderFailure:
                self.c_trigger_event(ORDER_FAILURE_EVENT,
                                     MarketOrderFailureEvent(self._current_timestamp,
                                                             tracked_order.client_order_id,
                                                             tracked_order.order_type))

            # Complete the order if relevent
            if tracked_order.is_done:
                if not tracked_order.is_failure:
                    if tracked_order.trade_type is TradeType.BUY:
                        self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                           f"according to user stream.")
                        self.c_trigger_event(BUY_ORDER_COMPLETED_EVENT,
                                             BuyOrderCompletedEvent(self._current_timestamp,
                                                                    tracked_order.client_order_id,
                                                                    tracked_order.base_asset,
                                                                    tracked_order.quote_asset,
                                                                    tracked_order.fee_asset,
                                                                    tracked_order.executed_amount_base,
                                                                    tracked_order.executed_amount_quote,
                                                                    tracked_order.fee_paid,
                                                                    tracked_order.order_type))
                    else:
                        self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                           f"according to user stream.")
                        self.c_trigger_event(SELL_ORDER_COMPLETED_EVENT,
                                             SellOrderCompletedEvent(self._current_timestamp,
                                                                     tracked_order.client_order_id,
                                                                     tracked_order.base_asset,
                                                                     tracked_order.quote_asset,
                                                                     tracked_order.fee_asset,
                                                                     tracked_order.executed_amount_base,
                                                                     tracked_order.executed_amount_quote,
                                                                     tracked_order.fee_paid,
                                                                     tracked_order.order_type))
                else:
                    # check if its a cancelled order
                    # if its a cancelled order, check in flight orders
                    # if present in in flight orders issue cancel and stop tracking order
                    if tracked_order.is_cancelled:
                        if tracked_order.client_order_id in self._in_flight_orders:
                            self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id}.")
                    else:
                        self.logger().info(f"The market order {tracked_order.client_order_id} has failed according to "
                                           f"order status API.")

                self.c_stop_tracking_order(tracked_order.client_order_id)

    async def _set_balances(self, updates, is_snapshot=False):
        try:
            tokens = set(self.token_configuration.get_tokens())
            if len(tokens) == 0:
                await self.token_configuration._configure()
                tokens = set(self.token_configuration.get_tokens())

            async with self._lock:
                if is_snapshot:
                    for market in updates.keys():
                        data = updates[market]

                        padded_total_amount: str = data['wei']
                        token_id: int = data['marketId']
                        if token_id in self._token_configuration._symbol_lookup:
                            token_symbol: str = self._token_configuration.get_symbol(token_id)
                        
                            total_amount: Decimal = self._token_configuration.unpad(padded_total_amount, token_id)        

                            self._account_balances[token_symbol] = total_amount
                            self._account_available_balances[token_symbol] = total_amount
                elif 'balanceUpdate' in updates:
                    data = updates['balanceUpdate']
                    padded_total_amount: str = data['newWei']
                    token_id: int = data['marketId']
                    if token_id in tokens:
                        token_symbol: str = self._token_configuration.get_symbol(token_id)
                        total_amount: Decimal = self._token_configuration.unpad(padded_total_amount, token_id)        

                        self._account_balances[token_symbol] = total_amount
                        self._account_available_balances[token_symbol] = total_amount
                
        except Exception as e:
            self.logger().error(f"Could not set balance {repr(e)}")

    # ----------------------------------------
    # User stream updates

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from dydx. Check API key and network connection."
                )
                await asyncio.sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event: Dict[str, Any] = event_message
                topic: str = event['channel']
                data: Dict[str, Any] = event['contents']
                if topic == 'balance_updates':
                    await self._set_balances(data, is_snapshot=False)
                elif topic == 'orders':
                    exchange_order_id: str = data['order']['id']

                    for o in self._in_flight_orders:
                        if o.exchange_order_id == exchange_order_id:
                            tracked_order: DydxInFlightOrder = o
                            break

                    if tracked_order is None:
                        self.logger().warning(f"Unrecognized order ID from user stream: {client_order_id}.")
                        self.logger().warning(f"Event: {event_message}")
                        continue

                    # update the tracked order
                    self._update_inflight_order(tracked_order, data['order'])
                else:
                    self.logger().debug(f"Unrecognized user stream event topic: {topic}.")

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await asyncio.sleep(5.0)

    # ----------------------------------------
    # Polling Updates

    async def _polling_update(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await asyncio.gather(
                    self._update_balances(),
                    self._update_trading_rules(),
                    self._update_order_status(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning("Failed to fetch updates on dydx. Check network connection.")
                self.logger().info(e)

    async def _update_balances(self):
        wallet_address = self._dydx_auth.generate_auth_dict()['wallet_address']
        balances_response = await self.api_request("GET", f"{BALANCES_INFO_ROUTE}".replace(':wallet',wallet_address))
        await self._set_balances(balances_response['accounts'][0]["balances"], True)

    async def _update_trading_rules(self):
        markets_info = await self.api_request("GET", f"{MARKETS_INFO_ROUTE}")

        markets_info = markets_info["markets"]

        for market_name in markets_info:
            market = markets_info[market_name]
            if "baseCurrency" in market:
                baseid, quoteid = market['baseCurrency']['soloMarketId'], market['quoteCurrency']['soloMarketId']
                decimals = market['baseCurrency']['decimals']
                try:
                    price_increment=Decimal(self.token_configuration.pad(self.token_configuration.unpad(market['minimumTickSize'], baseid), quoteid))
                    self._trading_rules[f"{market_name}-limit"] = TradingRule(
                        trading_pair=market_name,
                        min_order_size =Decimal(self.token_configuration.unpad(market['smallOrderThreshold'], baseid)),
                        min_price_increment=price_increment,
                        min_base_amount_increment=Decimal(f"1e-{decimals}"),
                        min_notional_size = Decimal(self.token_configuration.unpad(market['smallOrderThreshold'], baseid)) * price_increment,
                        supports_limit_orders = True,
                        supports_market_orders = False
                    )
                    self._trading_rules[f"{market_name}-market"] = TradingRule(
                        trading_pair=market_name,
                        min_order_size =Decimal(self.token_configuration.unpad(market['minimumOrderSize'], baseid)),
                        min_price_increment=price_increment,
                        min_base_amount_increment=Decimal(f"1e-{decimals}"),
                        min_notional_size = Decimal(self.token_configuration.unpad(market['minimumOrderSize'], baseid)) * price_increment,
                        supports_limit_orders = False,
                        supports_market_orders = True
                    )
                    self._fee_rules[market_name] = {
                      "makerFee": market["makerFee"],
                      "largeTakerFee": market["largeTakerFee"],
                      "smallTakerFee": market["smallTakerFee"]
                    }
                except Exception as e:
                    self.logger().warning("Error updating trading rules")
                    self.logger().warning(str(e))

    async def _update_order_status(self):
        tracked_orders = self._in_flight_orders.copy()

        for client_order_id, tracked_order in tracked_orders.iteritems():
            dydx_order_id = tracked_order.exchange_order_id
            if dydx_order_id is None:
                # This order is still pending acknowledgement from the exchange
                if tracked_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                    # this order should have a dydx_order_id at this point. If it doesn't, we should cancel it
                    # as we won't be able to poll for updates
                    try:
                        self.cancel_order(client_order_id)
                    except Exception:
                        pass
                continue 

            try:
                dydx_order_request = await self.api_request("GET", f"{MAINNET_API_REST_ENDPOINT}{GET_ORDER_ROUTE}/{dydx_order_id}")
                data = dydx_order_request["order"]
            except Exception:
                self.logger().warning(f"Failed to fetch tracked dydx order " \
                                      f"{client_order_id }({tracked_order.exchange_order_id}) from api (code: {dydx_order_request['resultInfo']['code']})")

                # check if this error is because the api cliams to be unaware of this order. If so, and this order
                # is reasonably old, mark the orde as cancelled
                if "error" in dydx_order_request:
                    if tracked_order.created_at < (int(time.time()) - UNRECOGNIZED_ORDER_DEBOUCE):
                        self.logger().warning(f"marking {client_order_id} as cancelled")
                        cancellation_event = OrderCancelledEvent(now(), client_order_id)
                        self.c_trigger_event(ORDER_CANCELLED_EVENT, cancellation_event)
                        self.stop_tracking(client_order_id)
                continue

            try:
                self._update_inflight_order(tracked_order, data)
            except Exception as e:
                self.logger().error(f"Failed to update dydx order {tracked_order.exchange_order_id}")
                self.logger().error(e)

    # ==========================================================
    # Miscellaneous
    # ----------------------------------------------------------

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        return self._trading_rules[f"{trading_pair}-limit"].min_price_increment

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        return self._trading_rules[f"{trading_pair}-limit"].min_base_amount_increment

    cdef object c_quantize_order_price(self, str trading_pair, object price):
        return price.quantize(self.c_get_order_price_quantum(trading_pair, price))

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price = 0.0):
        quantized_amount = amount.quantize(self.c_get_order_size_quantum(trading_pair, amount))
        rules = self._trading_rules[f"{trading_pair}-market"]

        if quantized_amount < rules.min_order_size:
            return s_decimal_0

        if price > 0 and price * quantized_amount < rules.min_notional_size:
            return s_decimal_0

        return quantized_amount

    cdef c_tick(self, double timestamp):
        cdef:
            int64_t last_tick = <int64_t> (self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t> (timestamp / self._poll_interval)

        self._tx_tracker.c_tick(timestamp)
        ExchangeBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def api_request(self,
                          http_method: str,
                          url: str,
                          data: Optional[Dict[str, Any]] = None,
                          params: Optional[Dict[str, Any]] = None,
                          headers: Optional[Dict[str, str]] = {},
                          secure: bool = False) -> Dict[str, Any]:

        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()

        if data is not None and http_method == "POST":
            data = json.dumps(data).encode('utf8')
            headers = {"Content-Type": "application/json"}

        headers.update(self._dydx_auth.generate_auth_dict())
        full_url = f"{self.API_REST_ENDPOINT}{url}"

        async with self._shared_client.request(http_method, url=full_url,
                                               timeout=API_CALL_TIMEOUT,
                                               data=data, params=params, headers=headers) as response:
            if response.status > 299:
                self.logger().info(f"Issue with dydx API {http_method} to {url}, response: ")
                self.logger().info(await response.text())
                raise IOError(f"Error fetching data from {full_url}. HTTP status is {response.status}.")
            data = await response.json()
            return data

    def get_order_book(self, trading_pair: str) -> OrderBook:
        return self.c_get_order_book(trading_pair)

    def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        return self.c_get_price(trading_pair, is_buy)

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        return self.c_buy(trading_pair, amount, order_type, price, kwargs)

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        return self.c_sell(trading_pair, amount, order_type, price, kwargs)

    def cancel(self, trading_pair: str, client_order_id: str):
        return self.c_cancel(trading_pair, client_order_id)

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        return self.c_get_fee(base_currency, quote_currency, order_type, order_side, amount, price)

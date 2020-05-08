# distutils: language=c++
import logging
from decimal import Decimal
import pandas as pd
from typing import (
    List,
    Tuple,
)

from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.event.events import (
    TradeType,
    OrderType,
)
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy import market_trading_pair_tuple
from hummingbot.strategy.strategy_base import StrategyBase
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.liquidity_mirroring.liquidity_mirroring_market_pair import LiquidityMirroringMarketPair
from hummingbot.core.utils.exchange_rate_conversion import ExchangeRateConversion

NaN = float("nan")
s_decimal_0 = Decimal(0)
as_logger = None


cdef class LiquidityMirroringStrategy(StrategyBase):
    OPTION_LOG_STATUS_REPORT = 1 << 0
    OPTION_LOG_CREATE_ORDER = 1 << 1
    OPTION_LOG_ORDER_COMPLETED = 1 << 2
    OPTION_LOG_PROFITABILITY_STEP = 1 << 3
    OPTION_LOG_FULL_PROFITABILITY_STEP = 1 << 4
    OPTION_LOG_INSUFFICIENT_ASSET = 1 << 5
    OPTION_LOG_ALL = 0xfffffffffffffff
    MARKET_ORDER_MAX_TRACKING_TIME = 60.0 * 10
    FAILED_ORDER_COOL_OFF_TIME = 60.0 * 30

    @classmethod
    def logger(cls):
        global as_logger
        if as_logger is None:
            as_logger = logging.getLogger(__name__)
        return as_logger

    def __init__(self,
                 primary_market_pairs: List[MarketTradingPairTuple],
                 mirrored_market_pairs: List[MarketTradingPairTuple],
                 two_sided_mirroring: bool,
                 spread_percent: float,
                 max_exposure_base: float,
                 max_exposure_quote: float,
                 max_loss: float,
                 max_total_loss: float,
                 equivalent_tokens: list,
                 logging_options: int = OPTION_LOG_ORDER_COMPLETED,
                 status_report_interval: float = 60.0,
                 next_trade_delay_interval: float = 15.0,
                 failed_order_tolerance: int = 1):
        """
        :param market_pairs: list liquidity mirroring market pairs
        :param logging_options: select the types of logs to output
        :param status_report_interval: how often to report network connection related warnings, if any
        :param next_trade_delay_interval: cool off period between trades
        :param failed_order_tolerance: number of failed orders to force stop the strategy when exceeded
        """

        super().__init__()
        self._logging_options = logging_options
        self.primary_market_pairs = primary_market_pairs
        self.mirrored_market_pairs = mirrored_market_pairs
        self._all_markets_ready = False
        self._status_report_interval = status_report_interval
        self._last_timestamp = 0
        self._next_trade_delay = next_trade_delay_interval
        self._last_trade_timestamps = {}
        self._failed_order_tolerance = failed_order_tolerance
        self._cool_off_logged = False
        self.two_sided_mirroring = two_sided_mirroring
        self._failed_market_order_count = 0
        self._last_failed_market_order_timestamp = 0
                                                                
        cdef:
            set all_markets = {
                primary_market_pairs[0].market,
                mirrored_market_pairs[0].market
            }

        self.c_add_markets(list(all_markets))
        
        # initialize the bounds of the orderbooks
        self.primary_best_bid = 0.0
        self.primary_best_ask = float("inf")
        self.mirrored_best_bid = 0.0
        self.mirrored_best_ask = float("inf")

        self.spread_percent = spread_percent
        self.max_exposure_base = max_exposure_base
        self.max_exposure_quote = max_exposure_quote

        self.bid_amount_percents = [float(1/55),float(2/55),float(3/55),float(4/55),float(5/55),float(6/55),
                               float(7/55),float(8/55),float(9/55),float(10/55)]
        self.ask_amount_percents = [float(1/55),float(2/55),float(3/55),float(4/55),float(5/55),float(6/55),
                               float(7/55),float(8/55),float(9/55),float(10/55)]

        self.bid_amounts = []
        self.ask_amounts = []
        for amount in self.bid_amount_percents:
            self.bid_amounts.append(amount * self.max_exposure_quote)
        for amount in self.ask_amount_percents:
            self.ask_amounts.append(amount * self.max_exposure_base)

        self.outstanding_offsets = {}
        self.max_loss = max_loss
        self.equivalent_tokens = equivalent_tokens
        self.current_total_offset_loss = 0
        self.amount_to_offset = 0
        self.max_total_loss = max_total_loss
        self.avg_sell_price = [0,0]
        self.avg_buy_price = [0,0]
        self.offset_base_exposure = 0
        self.offset_quote_exposure = 0
        assets_df = self.wallet_balance_data_frame([self.mirrored_market_pairs[0]])
        total_balance = assets_df['Total Balance']
        assets_df = self.wallet_balance_data_frame([self.primary_market_pairs[0]])
        total_balance += assets_df['Total Balance']
        self.initial_base_amount = total_balance[0]
        self.initial_quote_amount = total_balance[1]


    @property
    def tracked_taker_orders(self) -> List[Tuple[MarketBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_taker_orders

    @property
    def tracked_taker_orders_data_frame(self) -> List[pd.DataFrame]:
        return self._sb_order_tracker.tracked_taker_orders_data_frame

    def format_status(self) -> str:
        cdef:
            list lines = []
            list warning_lines = []
        total_balance = 0
        for market_pair in (self.primary_market_pairs + self.mirrored_market_pairs):
            warning_lines.extend(self.network_warning([market_pair]))
            markets_df = self.market_status_data_frame([market_pair])
            lines.extend(["", "  Markets:"] +
                         ["    " + line for line in str(markets_df).split("\n")])

            assets_df = self.wallet_balance_data_frame([market_pair])
            lines.extend(["", "  Assets:"] +
                         ["    " + line for line in str(assets_df).split("\n")])
            total_balance += assets_df['Total Balance']
            
            tracked_orders_df = self.tracked_taker_orders_data_frame
            if len(tracked_orders_df) > 0:
                df_lines = str(tracked_orders_df).split("\n")
                lines.extend(["", "  Pending market orders:"] +
                             ["    " + line for line in df_lines])
            else:
                lines.extend(["", "  No pending market orders."])

            warning_lines.extend(self.balance_warning([market_pair]))
        
        mirrored_market_df = self.market_status_data_frame([self.mirrored_market_pairs[0]])
        mult = mirrored_market_df["Best Bid Price"]

        profit = float((total_balance[0] - self.initial_base_amount) * mult) + float(total_balance[1] - self.initial_quote_amount)

        lines.extend(["", f"   Total Balance ({self.primary_market_pairs[0].base_asset}): {total_balance[0]}"])
        lines.extend(["", f"   Total Balance ({self.primary_market_pairs[0].quote_asset}): {total_balance[1]}"])
        lines.extend(["", f"   Estimated Profit: {profit}"])
        if len(warning_lines) > 0:
            lines.extend(["", "  *** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    cdef c_tick(self, double timestamp):
        """
        Clock tick entry point.

        For liquidity mirroring strategy, this function simply checks for the readiness and connection status of markets, and
        then delegates the processing of each market pair to c_process_market_pair().

        :param timestamp: current tick timestamp
        """
        StrategyBase.c_tick(self, timestamp)

        cdef:
            int64_t current_tick = <int64_t>(timestamp // self._status_report_interval)
            int64_t last_tick = <int64_t>(self._last_timestamp // self._status_report_interval)
            bint should_report_warnings = ((current_tick > last_tick) and
                                           (self._logging_options & self.OPTION_LOG_STATUS_REPORT))
        try:
            if not self._all_markets_ready:
                self._all_markets_ready = all([market.ready for market in self._sb_markets])
                if not self._all_markets_ready:
                    # Markets not ready yet. Don't do anything.
                    if should_report_warnings:
                        self.logger().warning(f"Markets are not ready. No trading is permitted.")
                    return
                else:
                    if self.OPTION_LOG_STATUS_REPORT:
                        self.logger().info(f"Markets are ready. Trading started.")

            if not all([market.network_status is NetworkStatus.CONNECTED for market in self._sb_markets]):
                if should_report_warnings:
                    self.logger().warning(f"Markets are not all online. No trading is permitted.")
                return
            if (self.current_total_offset_loss < self.max_total_loss):
                for market_pair in self.mirrored_market_pairs:
                    self.c_process_market_pair(market_pair)
            else:
                self.logger().warning("Too much total offset loss!")
        finally:
            self._last_timestamp = timestamp

    cdef c_did_complete_buy_order(self, object buy_order_completed_event):
        """
        Output log for completed buy order.

        :param buy_order_completed_event: Order completed event
        """
        cdef:
            str order_id = buy_order_completed_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                self.avg_buy_price[0] += float(buy_order_completed_event.quote_asset_amount)
                self.avg_buy_price[1] += float(buy_order_completed_event.base_asset_amount)
                self.amount_to_offset += float(buy_order_completed_event.base_asset_amount)
            else:
                if (self.avg_sell_price[1] > 0):
                        true_average = Decimal(self.avg_sell_price[0]/self.avg_sell_price[1])
                else:
                    true_average = Decimal(0)
                self.current_total_offset_loss += float(buy_order_completed_event.quote_asset_amount - (buy_order_completed_event.base_asset_amount * true_average))
                self.amount_to_offset += float(buy_order_completed_event.base_asset_amount)
                self.offset_quote_exposure -= float(buy_order_completed_event.quote_asset_amount)
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                    f"Limit order completed on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef c_did_complete_sell_order(self, object sell_order_completed_event):
        """
        Output log for completed sell order.

        :param sell_order_completed_event: Order completed event
        """
        cdef:
            str order_id = sell_order_completed_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            if market_trading_pair_tuple.market == self.primary_market_pairs[0].market:
                self.amount_to_offset -= float(sell_order_completed_event.base_asset_amount)
                self.avg_sell_price[0] += float(sell_order_completed_event.quote_asset_amount)
                self.avg_sell_price[1] += float(sell_order_completed_event.base_asset_amount)
            else:
                if (self.avg_buy_price[1] > 0):
                    true_average = Decimal(self.avg_buy_price[0]/self.avg_buy_price[1])
                else:
                    true_average = Decimal(0)
                self.current_total_offset_loss -= float(sell_order_completed_event.quote_asset_amount - (sell_order_completed_event.base_asset_amount * true_average))
                self.amount_to_offset -= float(sell_order_completed_event.base_asset_amount)
                self.offset_base_exposure -= float(sell_order_completed_event.base_asset_amount)
            if self._logging_options & self.OPTION_LOG_ORDER_COMPLETED:
                self.log_with_clock(logging.INFO,
                                    f"Limit order completed on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef c_did_fail_order(self, object fail_event):
        """
        Output log for failed order.

        :param fail_event: Order failure event
        """
        if fail_event.order_type is OrderType.LIMIT:
            self._failed_market_order_count += 1
            self._last_failed_market_order_timestamp = fail_event.timestamp

        if self._failed_market_order_count > self._failed_order_tolerance:
            failed_order_kill_switch_log = \
                f"Strategy is forced stop by failed order kill switch. " \
                f"Failed market order count {self._failed_market_order_count} exceeded tolerance lever of " \
                f"{self._failed_order_tolerance}. Please check market connectivity before restarting."

            self.logger().network(failed_order_kill_switch_log, app_warning_msg=failed_order_kill_switch_log)
            self.c_stop(self._clock)
        cdef:
            str order_id = fail_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            self.log_with_clock(logging.INFO,
                                f"Limit order failed on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef c_did_cancel_order(self, object cancel_event):
        """
        Output log for cancelled order.

        :param cancel_event: Order cancelled event.
        """
        cdef:
            str order_id = cancel_event.order_id
            object market_trading_pair_tuple = self._sb_order_tracker.c_get_market_pair_from_order_id(order_id)
        if market_trading_pair_tuple is not None:
            self.log_with_clock(logging.INFO,
                                f"Limit order canceled on {market_trading_pair_tuple[0].name}: {order_id}")

    cdef bint c_ready_for_new_orders(self, list market_trading_pair_tuples):
        """
        Check whether we are ready for making new mirroring orders or not. Conditions where we should not make further
        new orders include:

         1. There's an in-flight market order that's still being resolved.
         2. We're still within the cool-off period from the last trade, which means the exchange balances may be not
            accurate temporarily.

        If none of the above conditions are matched, then we're ready for new orders.

        :param market_trading_pair_tuples: list of mirroring market pairs
        :return: True if ready, False if not
        """
        cdef:
            double time_left
            dict tracked_taker_orders = self._sb_order_tracker.c_get_taker_orders()

        ready_ts_from_failed_order = self._last_failed_market_order_timestamp + \
            self._failed_market_order_count * self.FAILED_ORDER_COOL_OFF_TIME
        # Wait for FAILED_ORDER_COOL_OFF_TIME * failed_market_order_count before retrying
        if ready_ts_from_failed_order > self._current_timestamp:
            time_left = ready_ts_from_failed_order - self._current_timestamp
            if not self._cool_off_logged:
                self.log_with_clock(
                    logging.INFO,
                    f"Cooling off from failed order. "
                    f"Resuming in {int(time_left)} seconds."
                )
                self._cool_off_logged = True
            return False

        for market_trading_pair_tuple in market_trading_pair_tuples:
            # Do not continue if there are pending market order
            if len(tracked_taker_orders.get(market_trading_pair_tuple, {})) > 0:
                # consider market order completed if it was already x time old
                if any([order.timestamp - self._current_timestamp < self.MARKET_ORDER_MAX_TRACKING_TIME
                       for order in tracked_taker_orders[market_trading_pair_tuple].values()]):
                    return False
            # Wait for the cool off interval before the next trade, so wallet balance is up to date
            ready_to_trade_time = self._last_trade_timestamps.get(market_trading_pair_tuple, 0) + self._next_trade_delay
            if market_trading_pair_tuple in self._last_trade_timestamps and ready_to_trade_time > self._current_timestamp:
                time_left = self._current_timestamp - self._last_trade_timestamps[market_trading_pair_tuple] - self._next_trade_delay
                if not self._cool_off_logged:
                    self.log_with_clock(
                        logging.INFO,
                        f"Cooling off from previous trade on {market_trading_pair_tuple.market.name}. "
                        f"Resuming in {int(time_left)} seconds."
                    )
                    self._cool_off_logged = True
                return False

        if self._cool_off_logged:
            self.log_with_clock(
                logging.INFO,
                f"Cool off completed. Liquidity Mirroring strategy is now ready for new orders."
            )
            # reset cool off log tag when strategy is ready for new orders
            self._cool_off_logged = False

        return True

    cdef c_process_market_pair(self, object market_pair):
        primary_market_pair = self.primary_market_pairs[0]

        bids = list(market_pair.order_book_bid_entries())
        best_bid = bids[0]

        asks = list(market_pair.order_book_ask_entries())
        best_ask = asks[0]

        # ensure we are looking at levels and not just orders
        bid_levels = [{"price": best_bid.price, "amount": best_bid.amount}]
        i = 1
        current_level = 0
        current_bid_price = best_bid.price
        while (len(bid_levels) < 10):
            if (bids[i].price == current_bid_price):
                bid_levels[current_level]["amount"] += bids[i].amount
                i += 1
            else:
                current_level += 1
                bid_levels.append({"price": bids[i].price, "amount": bids[i].amount})
                current_bid_price = bids[i].price
                i += 1

        # ensure we are looking at levels and not just orders
        ask_levels = [{"price": best_ask.price, "amount": best_ask.amount}]
        i = 1
        current_level = 0
        current_ask_price = best_ask.price
        while (len(ask_levels) < 10):
            if (asks[i].price == current_ask_price):
                ask_levels[current_level]["amount"] += asks[i].amount
                i += 1
            else:
                current_level += 1
                ask_levels.append({"price": asks[i].price, "amount": asks[i].amount})
                current_ask_price = asks[i].price
                i += 1

        self.adjust_primary_orderbook(primary_market_pair, best_bid, best_ask, bid_levels, ask_levels)
        if (self.c_ready_for_new_orders([market_pair])):
            self.adjust_mirrored_orderbook(market_pair, best_bid, best_ask)

    def adjust_primary_orderbook(self, primary_market_pair, best_bid, best_ask, bids, asks):
        primary_market: MarketBase = primary_market_pair.market

        available_quote_exposure = self.max_exposure_quote - self.offset_quote_exposure
        available_base_exposure = self.max_exposure_base - self.offset_base_exposure
        
        for j in range(0,len(self.bid_amount_percents)):
            self.bid_amounts[j] = (self.bid_amount_percents[j] * available_quote_exposure)
        
        for j in range(0,len(self.ask_amount_percents)):
            self.ask_amounts[j] = (self.ask_amount_percents[j] * available_base_exposure)


        spread = float(best_ask.price - best_bid.price)
        spread_factor = (spread)/float(best_ask.price)
        if spread_factor < self.spread_percent:
            adjustment_factor = (self.spread_percent*float(best_ask.price) - spread)/(2-self.spread_percent)
            adjusted_ask = float(best_ask.price) + adjustment_factor
            adjusted_bid = float(best_bid.price) - adjustment_factor
        else:
            adjusted_ask = float(best_ask.price)
            adjusted_bid = float(best_bid.price)

        bid_price_diff = abs(1 - (self.primary_best_bid/adjusted_bid))
        ask_price_diff = abs(1 - (self.primary_best_ask/adjusted_ask))

        active_orders = self._sb_order_tracker.market_pair_to_active_orders

        if (bid_price_diff > self.spread_percent):
            self.primary_best_bid = adjusted_bid
            bid_inc = self.primary_best_bid * 0.001
            if primary_market_pair in active_orders:
                for order in active_orders[primary_market_pair]:
                    if order.is_buy:
                        self.c_cancel_order(primary_market_pair,order.client_order_id)
            amount = Decimal(min(best_bid.amount, (self.bid_amounts[0]/adjusted_bid)))
            
            fee_object = primary_market.c_get_fee(
                    primary_market_pair.base_asset,
                    primary_market_pair.quote_asset,
                    OrderType.LIMIT,
                    TradeType.BUY,
                    amount,
                    adjusted_bid
                )
            
            total_flat_fees = self.c_sum_flat_fees(primary_market_pair.quote_asset,
                                                       fee_object.flat_fees)
            fixed_cost_per_unit = total_flat_fees / amount                                                       

            price_tx = Decimal(adjusted_bid) / (Decimal(1) + fee_object.percent) - fixed_cost_per_unit
            quant_price = primary_market.c_quantize_order_price(primary_market_pair.trading_pair, price_tx)
            quant_amount = primary_market.c_quantize_order_amount(primary_market_pair.trading_pair, amount)
            self.c_buy_with_specific_market(primary_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))
            
            price = self.primary_best_bid
            for i in range(0,8):
                price -= bid_inc
                min_price = min(price, bids[i+1]["price"])
                amount = Decimal(min(bids[i+1]["amount"], (self.bid_amounts[i+1]/adjusted_bid)))
                
                fee_object = primary_market.c_get_fee(
                    primary_market_pair.base_asset,
                    primary_market_pair.quote_asset,
                    OrderType.LIMIT,
                    TradeType.BUY,
                    amount,
                    min_price
                )
            
                total_flat_fees = self.c_sum_flat_fees(primary_market_pair.quote_asset,
                                                           fee_object.flat_fees)

                fixed_cost_per_unit = total_flat_fees / amount                                                           

                min_price = Decimal(min_price) / (Decimal(1) + fee_object.percent) - fixed_cost_per_unit
                quant_price = primary_market.c_quantize_order_price(primary_market_pair.trading_pair, min_price)
                quant_amount = primary_market.c_quantize_order_amount(primary_market_pair.trading_pair, amount)
                self.c_buy_with_specific_market(primary_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))
        
        if (ask_price_diff > self.spread_percent):
            self.primary_best_ask = adjusted_ask
            ask_inc = self.primary_best_ask * 0.001
            if primary_market_pair in active_orders:
                for order in active_orders[primary_market_pair]:
                    if not order.is_buy:
                        self.c_cancel_order(primary_market_pair,order.client_order_id)
            amount = Decimal(min(best_ask.amount, self.ask_amounts[0]))

            fee_object = primary_market.c_get_fee(
                    primary_market_pair.base_asset,
                    primary_market_pair.quote_asset,
                    OrderType.LIMIT,
                    TradeType.SELL,
                    amount,
                    adjusted_ask
                )
            
            total_flat_fees = self.c_sum_flat_fees(primary_market_pair.quote_asset,
                                                       fee_object.flat_fees)
            fixed_cost_per_unit = total_flat_fees / amount                                                       

            price_tx = Decimal(adjusted_ask) / (Decimal(1) - fee_object.percent) + fixed_cost_per_unit

            quant_price = primary_market.c_quantize_order_price(primary_market_pair.trading_pair, price_tx)
            quant_amount = primary_market.c_quantize_order_amount(primary_market_pair.trading_pair, amount)
            self.c_sell_with_specific_market(primary_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))

            price = self.primary_best_ask
            for i in range(0,8):
                price += ask_inc
                max_price = max(price, asks[i+1]["price"])
                amount = Decimal(min(asks[i+1]["amount"], self.ask_amounts[i+1]))
                
                fee_object = primary_market.c_get_fee(
                    primary_market_pair.base_asset,
                    primary_market_pair.quote_asset,
                    OrderType.LIMIT,
                    TradeType.SELL,
                    amount,
                    max_price
                )
            
                total_flat_fees = self.c_sum_flat_fees(primary_market_pair.quote_asset,
                                                           fee_object.flat_fees)

                fixed_cost_per_unit = total_flat_fees / amount                                                           

                max_price = Decimal(max_price) / (Decimal(1) - fee_object.percent) + fixed_cost_per_unit
                quant_price = primary_market.c_quantize_order_price(primary_market_pair.trading_pair, max_price)
                quant_amount = primary_market.c_quantize_order_amount(primary_market_pair.trading_pair, amount)
                self.c_sell_with_specific_market(primary_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))

    def adjust_mirrored_orderbook(self,mirrored_market_pair,best_bid,best_ask):
        mirrored_market: MarketBase = mirrored_market_pair.market
        
        if self.amount_to_offset == 0:
            return
        else:
            current_exposure = 0.0
            active_orders = self._sb_order_tracker.market_pair_to_active_orders
            current_orders = []
            if mirrored_market_pair in active_orders:
                current_orders = active_orders[mirrored_market_pair][:]
            
            if self.amount_to_offset < 0:
                # we are at a deficit of base. get rid of sell orders
                for order in current_orders:
                    if not order.is_buy:
                        self.offset_base_exposure -= float(order.quantity)
                        self.c_cancel_order(mirrored_market_pair,order.client_order_id)
                    else:
                        if ((order.price - best_ask.price) > self.max_loss):
                            self.offset_quote_exposure -= float(order.quantity * order.price)
                            self.c_cancel_order(mirrored_market_pair,order.client_order_id)
                        else:
                            current_exposure += float(order.quantity)

                amount = ((-1) * self.amount_to_offset) - current_exposure
                if amount > self.two_sided_mirroring:
                    if (self.avg_sell_price[1] > 0):
                        true_average = Decimal(self.avg_sell_price[0]/self.avg_sell_price[1])
                    else:
                        true_average = Decimal(0)
                    new_price = best_ask.price
                    diff = float(new_price - true_average)
                    loss = diff * amount
                    if loss < self.max_loss:

                        fee_object = mirrored_market.c_get_fee(
                            mirrored_market_pair.base_asset,
                            mirrored_market_pair.quote_asset,
                            OrderType.LIMIT,
                            TradeType.BUY,
                            amount,
                            new_price
                        )

                        total_flat_fees = self.c_sum_flat_fees(mirrored_market_pair.quote_asset,
                                                                   fee_object.flat_fees)

                        fixed_cost_per_unit = total_flat_fees / Decimal(amount)                                                           

                        new_price = Decimal(new_price) / (Decimal(1) + fee_object.percent) - fixed_cost_per_unit
                        quant_price = mirrored_market.c_quantize_order_price(mirrored_market_pair.trading_pair, new_price)
                        quant_amount = mirrored_market.c_quantize_order_amount(mirrored_market_pair.trading_pair, amount)
                        
                        self.c_buy_with_specific_market(mirrored_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))
                        self.offset_quote_exposure += float(new_price) * amount
                    else:
                        self.logger().warning("TOO LOSSY!")

            elif self.amount_to_offset > 0:
            # we are at a surplus of base. get rid of buy orders
                for order in current_orders:
                    if order.is_buy:
                        self.offset_quote_exposure -= float(order.quantity * order.price)
                        self.c_cancel_order(mirrored_market_pair,order.client_order_id)
                    else:
                        if ((order.price - best_bid.price) > self.max_loss):
                            self.offset_base_exposure -= float(order.quantity * order.price)
                            self.c_cancel_order(mirrored_market_pair,order.client_order_id)
                        else:
                            current_exposure += float(order.quantity)

                amount = (self.amount_to_offset) - current_exposure
                if amount > self.two_sided_mirroring:
                    if (self.avg_buy_price[1] > 0):
                        true_average = Decimal(self.avg_buy_price[0]/self.avg_buy_price[1])
                    else:
                        true_average = Decimal(0)
                    new_price = best_bid.price
                    diff = float(true_average - new_price)
                    loss = diff * amount
                    if loss < self.max_loss:
                        fee_object = mirrored_market.c_get_fee(
                            mirrored_market_pair.base_asset,
                            mirrored_market_pair.quote_asset,
                            OrderType.LIMIT,
                            TradeType.SELL,
                            amount,
                            new_price
                        )

                        total_flat_fees = self.c_sum_flat_fees(mirrored_market_pair.quote_asset,
                                                                   fee_object.flat_fees)

                        fixed_cost_per_unit = total_flat_fees / Decimal(amount)                                                           

                        new_price = Decimal(new_price) / (Decimal(1) - fee_object.percent) + fixed_cost_per_unit
                        quant_price = mirrored_market.c_quantize_order_price(mirrored_market_pair.trading_pair, new_price)
                        quant_amount = mirrored_market.c_quantize_order_amount(mirrored_market_pair.trading_pair, amount)

                        self.c_sell_with_specific_market(mirrored_market_pair,Decimal(quant_amount),OrderType.LIMIT,Decimal(quant_price))
                        self.offset_base_exposure += amount
                    else:
                        self.logger().warning("TOO LOSSY!")

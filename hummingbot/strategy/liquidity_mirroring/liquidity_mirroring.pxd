# distutils: language=c++

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.strategy.strategy_base cimport StrategyBase
from libc.stdint cimport int64_t


cdef class LiquidityMirroringStrategy(StrategyBase):
    cdef:
        list mirrored_market_pairs
        list primary_market_pairs
        list bid_amounts
        list ask_amounts
        list equivalent_tokens
        list marked_for_deletion
        list has_been_offset
        str slack_url
        object performance_logger
        float best_bid_start
        float initial_base_amount
        float initial_quote_amount
        float amount_to_offset
        float current_total_offset_loss
        bint two_sided_mirroring
        float primary_best_bid
        float primary_best_ask
        float mirrored_best_bid
        float mirrored_best_ask
        float spread_percent
        float max_exposure_base
        float max_exposure_quote
        float max_loss
        float max_total_loss
        float total_trading_volume
        float trades_executed
        float offset_base_exposure
        float offset_quote_exposure
        float min_primary_amount
        float min_mirroring_amount
        list avg_buy_price
        list avg_sell_price
        list bid_amount_percents
        list ask_amount_percents
        bint _all_markets_ready
        dict outstanding_offsets
        dict _order_id_to_market
        dict market_orderbook_heaps
        double _status_report_interval
        double _last_timestamp
        dict _last_trade_timestamps
        double _next_trade_delay
        set _sell_markets
        set _buy_markets
        int64_t _logging_options
        object _exchange_rate_conversion
        int _failed_order_tolerance
        bint _cool_off_logged
        int _failed_market_order_count
        int _last_failed_market_order_timestamp
        int cycle_number

    cdef c_process_market_pair(self, object market_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)

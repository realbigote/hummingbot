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
        float two_sided_mirroring
        float primary_best_bid
        float primary_best_ask
        float mirrored_best_bid
        float mirrored_best_ask
        float spread_percent
        float max_exposure_base
        float max_exposure_quote
        bint _all_markets_ready
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

    cdef c_process_market_pair(self, object market_pair)
    cdef bint c_ready_for_new_orders(self, list market_trading_pairs)
